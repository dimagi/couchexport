import hashlib
from itertools import islice
from urllib2 import URLError
from couchdbkit.ext.django.schema import Document, DictProperty,\
    DocumentSchema, StringProperty, SchemaListProperty, ListProperty,\
    StringListProperty, DateTimeProperty, SchemaProperty, BooleanProperty
import json
from StringIO import StringIO
import couchexport
from couchexport.transforms import identity
from couchexport.util import SerializableFunctionProperty,\
    get_schema_index_view_keys, force_tag_to_list
from dimagi.utils.decorators.memoized import memoized
from dimagi.utils.mixins import UnicodeMixIn
from dimagi.utils.couch.database import get_db, iter_docs
from soil import DownloadBase
from couchdbkit.exceptions import ResourceNotFound
from couchexport.properties import TimeStampProperty, JsonProperty
from couchdbkit.consumer import Consumer
from dimagi.utils.logging import notify_exception

class Format(object):
    """
    Supported formats go here.
    """
    CSV = "csv"
    ZIP = "zip"
    XLS = "xls"
    XLS_2007 = "xlsx"
    HTML = "html"
    JSON = "json"
    
    FORMAT_DICT = {CSV: {"mimetype": "application/zip",
                         "extension": "zip",
                         "download": True},
                   ZIP: {"mimetype": "application/zip",
                         "extension": "zip",
                         "download": True},
                   XLS: {"mimetype": "application/vnd.ms-excel",
                         "extension": "xls",
                         "download": True},
                   XLS_2007: {"mimetype": "application/vnd.ms-excel",
                              "extension": "xlsx",
                              "download": True},
                   HTML: {"mimetype": "text/html",
                          "extension": "html",
                          "download": False},
                   JSON: {"mimetype": "application/json",
                          "extension": "json",
                          "download": False}}
    
    VALID_FORMATS = FORMAT_DICT.keys()
    
    def __init__(self, slug, mimetype, extension, download):
        self.slug = slug
        self.mimetype = mimetype
        self.extension = extension
        self.download = download
    
    @classmethod
    def from_format(cls, format):
        format = format.lower()
        if format not in cls.VALID_FORMATS:
            raise URLError("Unsupported export format: %s!" % format)
        return cls(format, **cls.FORMAT_DICT[format])

class ExportSchema(Document, UnicodeMixIn):
    """
    An export schema that can store intermittent contents of the export so
    that the entire doc list doesn't have to be used to generate the export
    """
    index = JsonProperty()
    seq = StringProperty() # semi-deprecated
    schema = DictProperty()
    timestamp = TimeStampProperty()

    def __unicode__(self):
        return "%s: %s" % (json.dumps(self.index), self.seq)

    @property
    def is_bigcouch(self):
        try:
            int(self.seq)
            return False
        except ValueError:
            return True

    @classmethod
    def wrap(cls, data):
        ret = super(ExportSchema, cls).wrap(data)
        if not ret.timestamp:
            # these won't work on bigcouch so we want to know if this happens
            notify_exception(
                None,
                'an export without a timestamp was accessed! %s (%s)' % (ret.index, ret._id)
            )
            # this isn't the cleanest nor is it perfect but in the event
            # this doc traversed databases somehow and now has a bad seq
            # id, make sure to just reset it to 0.
            # This won't catch if the seq is bad but not greater than the
            # current one).
            current_seq = cls.get_db().info()["update_seq"]
            try:
                if int(current_seq) < int(ret.seq):
                    ret.seq = "0"
                    ret.save()
            except ValueError:
                # seqs likely weren't ints (e.g. bigcouch)
                # this should never be possible (anything on bigcouch should
                # have a timestamp) so let's fail hard
                raise Exception('export %s is in a bad state (no timestamp or integer seq)' % ret._id)
        # TODO? handle seq -> datetime migration
        return ret

    @classmethod
    def last(cls, index):
        # search first by timestamp, then fall back to seq id
        shared_kwargs = {
            'descending': True,
            'limit': 1,
            'include_docs': True,
            'reduce': False,
        }
        ret = cls.view("couchexport/schema_checkpoints",
                       startkey=['by_timestamp', json.dumps(index), {}],
                       endkey=['by_timestamp', json.dumps(index)],
                       **shared_kwargs).one()
        if ret and not ret.timestamp:
            # we found a bunch of old checkpoints but they only
            # had seq ids, so use those instead
            ret = cls.view("couchexport/schema_checkpoints",
                           startkey=['by_seq', json.dumps(index), {}],
                           endkey=['by_seq', json.dumps(index)],
                           **shared_kwargs).one()
        return ret

    @classmethod
    def get_all_indices(cls):
        ret = cls.get_db().view("couchexport/schema_checkpoints",
                                startkey=['by_timestamp'],
                                endkey=['by_timestamp', {}],
                                reduce=True,
                                group=True,
                                group_level=2)
        for row in ret:
            index = row['key'][1]
            try:
                yield json.loads(index)
            except ValueError:
                # ignore this for now - should just be garbage data
                # print "poorly formatted index key %s" % index
                pass

    @classmethod
    def get_all_checkpoints(cls, index):
        return cls.view("couchexport/schema_checkpoints",
                        startkey=['by_timestamp', json.dumps(index)],
                        endkey=['by_timestamp', json.dumps(index), {}],
                        include_docs=True,
                        reduce=False)

    _tables = None
    @property
    def tables(self):
        if self._tables is None:
            from couchexport.export import get_headers
            headers = get_headers(self.schema, separator=".")
            self._tables = [(index, row[0]) for index, row in headers]
        return self._tables

    @property
    def table_dict(self):
        return dict(self.tables)
    
    def get_columns(self, index):
        return ['id'] + self.table_dict[index].data

    def get_all_ids(self, database=None):
        database = database or self.get_db()
        return set(
            [result['id'] for result in database.view(
                        "couchexport/schema_index",
                        reduce=False,
                        **get_schema_index_view_keys(self.index)).all()])

    def get_new_ids(self, database=None):
        # TODO: deprecate/remove old way of doing this
        database = database or self.get_db()
        if self.timestamp:
            return self._ids_by_timestamp(database)
        else:
            return self._ids_by_seq(database)

    def _ids_by_seq(self, database):
        if self.seq == "0" or self.seq is None:
            return self.get_all_ids()

        consumer = Consumer(database)
        view_results = consumer.fetch(since=self.seq)
        if view_results:
            include_ids = set([res["id"] for res in view_results["results"]])
            return include_ids.intersection(self.get_all_ids())
        else:
            # sometimes this comes back empty. I think it might be a bug
            # in couchdbkit, but it's impossible to consistently reproduce.
            # For now, just assume this is fine.
            return set()

    def _ids_by_timestamp(self, database):
        tag_as_list = force_tag_to_list(self.index)
        startkey = tag_as_list + [self.timestamp.isoformat()]
        endkey = tag_as_list + [{}]
        return set(
            [result['id'] for result in database.view(
                        "couchexport/schema_index",
                        reduce=False,
                        startkey=startkey,
                        endkey=endkey)])

    def get_new_docs(self, database=None):
        return iter_docs(self.get_new_ids(database))

class ExportColumn(DocumentSchema):
    """
    A column configuration, for export
    """
    index = StringProperty()
    display = StringProperty()
    # signature: transform(val, doc) -> val
    transform = SerializableFunctionProperty(default=None)
    tag = StringProperty()
    is_sensitive = BooleanProperty(default=False)

    @classmethod
    def wrap(self, data):
        if 'is_sensitive' not in data and data.get('transform', None):
            data['is_sensitive'] = True
        return super(ExportColumn, self).wrap(data)

    def get_display(self):
         return '{primary}{extra}'.format(
             primary=self.display,
             extra=" [sensitive]" if self.is_sensitive else ''
         )

    def to_config_format(self, selected=True):
        return {
            "index": self.index,
            "display": self.display,
            "transform": self.transform.dumps() if self.transform else None,
            "is_sensitive": self.is_sensitive,
            "selected": selected,
            "tag": self.tag,
        }

class ExportTable(DocumentSchema):
    """
    A table configuration, for export
    """
    index = StringProperty()
    display = StringProperty()
    columns = SchemaListProperty(ExportColumn)
    order = ListProperty()

    @classmethod
    def wrap(cls, data):
        # hack: manually remove any references to _attachments at runtime
        data['columns'] = [c for c in data['columns'] if not c['index'].startswith("_attachments.")]
        return super(ExportTable, cls).wrap(data)

    @classmethod
    def default(cls, index):
        return cls(index=index, display="", columns=[])
        
    @property
    @memoized
    def displays_by_index(self):
        return dict((c.index, c.get_display()) for c in self.columns)
    
    def get_column_configuration(self, all_cols):
        selected_cols = set()
        for c in self.columns:
            selected_cols.add(c.index)
            yield c.to_config_format()

        for c in all_cols:
            if c not in selected_cols:
                column = ExportColumn(index=c)
                column.display = self.displays_by_index[c] if self.displays_by_index.has_key(c) else ''
                yield column.to_config_format(selected=False)

    def get_headers_row(self):
        from couchexport.export import FormattedRow
        headers = []
        for col in self.columns:
            display = self.displays_by_index[col.index]
            if col.index == 'id':
                id_len = len(
                    filter(lambda part: part == '#', self.index.split('.'))
                )
                headers.append(display)
                if id_len > 1:
                    for i in range(id_len):
                        headers.append('{id}__{i}'.format(id=display, i=i))
            else:
                headers.append(display)
        return FormattedRow(headers)

    @property
    @memoized
    def row_positions_by_index(self):
        return dict((h, i) for i, h in enumerate(self._headers) if self.displays_by_index.has_key(h))

    @property
    @memoized
    def id_index(self):
        for i, column in enumerate(self.columns):
            if column.index == 'id':
                return i

    def get_items_in_order(self, row):
        row_data = list(row.get_data())
        for column in self.columns:
            i = self.row_positions_by_index[column.index]
            val = row_data[i]
            yield column, val

    def trim(self, data, doc, apply_transforms, global_transform):
        from couchexport.export import FormattedRow, Constant, transform_error_constant
        if not hasattr(self, '_headers'):
            self._headers = tuple(data[0].get_data())

        # skip first element without copying
        data = islice(data, 1, None)

        for row in data:
            id = None
            cells = []
            for column, val in self.get_items_in_order(row):
                # DEID TRANSFORM BABY!
                if apply_transforms:
                    if column.transform and not isinstance(val, Constant):
                        try:
                            val = column.transform(val, doc)
                        except Exception:
                            val = transform_error_constant
                    elif global_transform:
                        val = global_transform(val, doc)

                if column.index == 'id':
                    id = val
                else:
                    cells.append(val)
            id_index = self.id_index if id else 0
            row_id = row.id if id else None
            yield FormattedRow(cells, row_id, id_index=id_index)

class BaseSavedExportSchema(Document):
    # signature: filter(doc)
    filter_function = SerializableFunctionProperty()

    @property
    def default_format(self):
        return Format.XLS_2007

    def transform(self, doc):
        return doc

    @property
    def filter(self):
        return self.filter_function

    @property
    def is_bulk(self):
        return False

    def export_data_async(self, format=None, **kwargs):
        format = format or self.default_format
        download = DownloadBase()
        download.set_task(couchexport.tasks.export_async.delay(
            self,
            download.download_id,
            format=format,
            **kwargs
        ))
        return download.get_start_response()

    @property
    def table_name(self):
        if len(self.index) > 2:
            return self.index[2]
        else:
            return "Form"

    def parse_headers(self, headers):
        return headers

    def parse_tables(self, tables):
        first_row = list(list(tables)[0])[1]
        return [(self.table_name, first_row)]

class FakeSavedExportSchema(BaseSavedExportSchema):
    index = JsonProperty()

    @property
    def name(self):
        return self.index

    @property
    def indices(self):
        return [self.index]

    def parse_headers(self, headers):
        first_header = headers[0][1]
        return [(self.table_name, first_header)]

    def get_export_components(self, previous_export_id=None, filter=None):
        from couchexport.export import get_export_components
        return get_export_components(self.index, previous_export_id, filter=self.filter & filter)

    def get_export_files(self, format='', previous_export_id=None, filter=None,
                         use_cache=True, max_column_size=2000, separator='|', process=None, **kwargs):
        # the APIs of how these methods are broken down suck, but at least
        # it's DRY
        from couchexport.export import export
        from django.core.cache import cache
        import hashlib

        export_tag = self.index

        CACHE_TIME = 1 * 60 * 60 # cache for 1 hour, in seconds
        def _build_cache_key(tag, prev_export_id, format, max_column_size):
            def _human_readable_key(tag, prev_export_id, format, max_column_size):
                return "couchexport_:%s:%s:%s:%s" % (tag, prev_export_id, format, max_column_size)
            return hashlib.md5(_human_readable_key(tag, prev_export_id,
                format, max_column_size)).hexdigest()

        # check cache, only supported for filterless queries, currently
        cache_key = _build_cache_key(export_tag, previous_export_id,
            format, max_column_size)
        if use_cache and filter is None:
            cached_data = cache.get(cache_key)
            if cached_data:
                (tmp, checkpoint) = cached_data
                return tmp, checkpoint

        tmp = StringIO()
        checkpoint = export(export_tag, tmp, format=format,
            previous_export_id=previous_export_id,
            filter=filter, max_column_size=max_column_size,
            separator=separator, export_object=self, process=process)

        if checkpoint:
            if use_cache:
                cache.set(cache_key, (tmp, checkpoint), CACHE_TIME)
            return tmp, checkpoint

        return None, None # hacky empty case


class SavedExportSchema(BaseSavedExportSchema, UnicodeMixIn):
    """
    Lets you save an export format with a schema and list of columns
    and display names.
    """

    name = StringProperty()
    default_format = StringProperty()

    is_safe = BooleanProperty(default=False)
    # self.index should always match self.schema.index
    # needs to be here so we can use in couch views
    index = JsonProperty()

    # id of an ExportSchema for checkpointed schemas
    schema_id = StringProperty()

    # user-defined table configuration
    tables = SchemaListProperty(ExportTable)

    # For us right now, 'form' or 'case'
    type = StringProperty()

    def __unicode__(self):
        return "%s (%s)" % (self.name, self.index)

    def transform(self, doc):
        return doc

    @property
    def global_transform_function(self):
        # will be called on every value in the doc during export
        return identity

    @property
    @memoized
    def schema(self):
        return ExportSchema.get(self.schema_id)

    @property
    def table_name(self):
        return self.sheet_name if self.sheet_name else "%s" % self._id

    @classmethod
    def default(cls, schema, name="", type='form'):
        return cls(name=name, index=schema.index, schema_id=schema.get_id,
                   tables=[ExportTable.default(schema.tables[0][0])], type=type)

    @property
    @memoized
    def tables_by_index(self):
        return dict([t.index, t] for t in self.tables)

    def get_table_configuration(self, index):
        def column_configuration():
            columns = self.schema.get_columns(index)
            if self.tables_by_index.has_key(index):
                return list(self.tables_by_index[index].get_column_configuration(columns))
            else:
                return [
                    ExportColumn(
                        index=c,
                        display=''
                    ).to_config_format(selected=False)
                    for c in columns
                ]

        def display():
            if self.tables_by_index.has_key(index):
                return self.tables_by_index[index].display
            else:
                return ''

        return {
            "index": index,
            "display": display(),
            "column_configuration": column_configuration(),
            "selected": index in self.tables_by_index
        }

    def get_table_headers(self, override_name=False):
        return ((self.table_name if override_name and i == 0 else t.index, [t.get_headers_row()]) for i, t in enumerate(self.tables))

    @property
    def table_configuration(self):
        return [self.get_table_configuration(index) for index, cols in self.schema.tables]

    def update_schema(self):
        """
        Update the schema for this object to include the latest columns from 
        any relevant docs.
        
        Does NOT save the doc, just updates the in-memory object.
        """
        from couchexport.schema import build_latest_schema
        self.set_schema(build_latest_schema(self.index))
        
    def set_schema(self, schema):
        """
        Set the schema for this object.
        
        Does NOT save the doc, just updates the in-memory object.
        """
        self.schema_id = schema.get_id
    
    def trim(self, document_table, doc, apply_transforms=True):
        for table_index, data in document_table:
            if self.tables_by_index.has_key(table_index):
                # todo: currently (index, rows) instead of (display, rows); where best to convert to display?
                yield (table_index, self.tables_by_index[table_index].trim(
                    data, doc, apply_transforms, self.global_transform_function
                ))


    def get_export_components(self, previous_export_id=None, filter=None):
        from couchexport.export import ExportConfiguration

        database = get_db()

        config = ExportConfiguration(database, self.index,
            previous_export_id,
            self.filter & filter)

        # get and checkpoint the latest schema
        updated_schema = config.get_latest_schema()
        export_schema_checkpoint = config.create_new_checkpoint()
        return config, updated_schema, export_schema_checkpoint

    def get_export_files(self, format=None, previous_export=None, filter=None, process=None, max_column_size=None,
                         apply_transforms=True, **kwargs):
        from couchexport.export import get_writer, format_tables, create_intermediate_tables

        if not format:
            format = self.default_format or Format.XLS_2007

        config, updated_schema, export_schema_checkpoint = self.get_export_components(previous_export, filter)

        # transform docs onto output and save
        writer = get_writer(format)
        
        # open the doc and the headers
        formatted_headers = list(self.get_table_headers())
        tmp = StringIO()
        writer.open(
            formatted_headers,
            tmp,
            max_column_size=max_column_size,
            table_titles=dict([
                (table.index, table.display)
                for table in self.tables if table.display
            ])
        )

        total_docs = len(config.potentially_relevant_ids)
        if process:
            DownloadBase.set_progress(process, 0, total_docs)
        for i, doc in config.enum_docs():
            if self.transform and apply_transforms:
                doc = self.transform(doc)
            formatted_tables = self.trim(
                format_tables(
                    create_intermediate_tables(doc, updated_schema),
                    separator="."
                ),
                doc,
                apply_transforms=apply_transforms
            )
            writer.write(formatted_tables)
            if process:
                DownloadBase.set_progress(process, i + 1, total_docs)

        writer.close()
        # hacky way of passing back the new format
        tmp.format = format
        return tmp, export_schema_checkpoint

    def download_data(self, format="", previous_export=None, filter=None):
        """
        If there is data, return an HTTPResponse with the appropriate data.
        If there is not data returns None.
        """
        from couchexport.shortcuts import export_response
        tmp, _ = self.get_export_files(format, previous_export, filter)
        return export_response(tmp, tmp.format, self.name)

    def to_export_config(self):
        """
        Return an ExportConfiguration object that represents this.
        """
        # confusingly, the index isn't the actual index property,
        # but is the index appended with the id to this document.
        # this is to avoid conflicts among multiple exports
        index = "%s-%s" % (self.index, self._id) if isinstance(self.index, basestring) else \
            self.index + [self._id] # self.index required to be a string or list
        return ExportConfiguration(index=index, name=self.name,
                                   format=self.default_format)

    # replaces `sheet_name = StringProperty()`

    def __get_sheet_name(self):
        return self.tables[0].display

    def __set_sheet_name(self, value):
        self.tables[0].display = value

    sheet_name = property(__get_sheet_name, __set_sheet_name)

    @classmethod
    def wrap(cls, data):
        # since this is a property now, trying to wrap it will fail hard
        if 'sheet_name' in data:
            del data['sheet_name']
        return super(SavedExportSchema, cls).wrap(data)


class ExportConfiguration(DocumentSchema):
    """
    Just a way to configure a single export. Used in the group export config.
    """
    index = JsonProperty()
    name = StringProperty()
    format = StringProperty()
    
    @property
    def filename(self):
        return "%s.%s" % (self.name, Format.from_format(self.format).extension)

    def __repr__(self):
        return ('%s (%s)' % (self.name, self.index)).encode('utf-8')

class GroupExportConfiguration(Document):
    """
    An export configuration allows you to setup a collection of exports
    that all run together. Used by the management command or a scheduled
    job to run a bunch of exports on a schedule.
    """
    full_exports = SchemaListProperty(ExportConfiguration)
    custom_export_ids = StringListProperty()
    
    def get_custom_exports(self):
        for custom in list(self.custom_export_ids):
            custom_export = self._get_custom(custom)
            if custom_export:
                yield custom_export

    def _get_custom(self, custom_id):
        """
        Get a custom export, or delete it's reference if not found
        """
        try:
            return SavedExportSchema.get(custom_id)
        except ResourceNotFound:
            try:
                self.custom_export_ids.remove(custom_id)
                self.save()
            except ValueError:
                pass

    @property
    def saved_exports(self):
        if not hasattr(self, "_saved_exports"):
            self._saved_exports = \
                [(export_config, 
                  SavedBasicExport.view("couchexport/saved_exports", 
                                        key=json.dumps(export_config.index),
                                        include_docs=True,
                                        reduce=False).one()) \
                 for export_config in self.all_configs]
        return self._saved_exports

    @property
    def all_configs(self):
        """
        Return an iterator of config-like objects that include the
        main configs + the custom export configs.
        """
        for full in self.full_exports:
            yield full
        for custom in self.get_custom_exports():
            yield custom.to_export_config()

    @property
    def all_export_schemas(self):
        """
        Return an iterator of ExportSchema-like objects that include the 
        main configs + the custom export configs.
        """
        for full in self.full_exports:
            yield FakeSavedExportSchema(index=full.index)
        for custom in self.get_custom_exports():
            yield custom

    @property
    def all_exports(self):
        """
        Returns an iterator of tuples consisting of the export config
        and an ExportSchema-like document that can be used to get at
        the data.
        """
        return zip(self.all_configs, self.all_export_schemas)

class SavedBasicExport(Document):
    """
    A cache of an export that lives in couch.
    Doesn't do anything smart, just works off an index
    """
    configuration = SchemaProperty(ExportConfiguration) 
    last_updated = DateTimeProperty()
    
    @property
    def size(self):
        try:
            return self._attachments[self.get_attachment_name()]["length"]
        except KeyError:
            return 0

    def has_file(self):
        return self.get_attachment_name() in self._attachments

    def get_attachment_name(self):
        # obfuscate this because couch doesn't like attachments that start with underscores
        return hashlib.md5(unicode(self.configuration.filename).encode('utf-8')).hexdigest()

    def set_payload(self, payload):
        self.put_attachment(payload, self.get_attachment_name())

    def get_payload(self):
        return self.fetch_attachment(self.get_attachment_name())
