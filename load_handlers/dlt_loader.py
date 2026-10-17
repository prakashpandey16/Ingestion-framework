import dlt
from load_handlers.base_loader import BaseLoader
import logging

logger = logging.getLogger(__name__)

class DLTAppendLoader(BaseLoader):
    @dlt.resource(write_disposition="append")
    def dlt_resource(data):
        return data

    def load_data_with_dlt(self, data, audit_id):
        data = self.add_common_columns(data, audit_id)
        pipeline = dlt.pipeline(name="append_pipeline", engine=self.engine)
        pipeline.run(self.dlt_resource(data))

class DLTTruncateLoader(BaseLoader):
    @dlt.resource(write_disposition="truncate")
    def dlt_resource(data):
        return data

    def load_data_with_dlt(self, data, audit_id):
        data = self.add_common_columns(data, audit_id)
        pipeline = dlt.pipeline(name="truncate_pipeline", engine=self.engine)
        pipeline.run(self.dlt_resource(data))

class DLTSCD2Loader(BaseLoader):
    @dlt.resource(write_disposition="merge")
    def dlt_resource(data):
        return data

    def load_data_with_dlt(self, data, audit_id):
        data = self.add_common_columns(data, audit_id)
        pipeline = dlt.pipeline(name="scd2_pipeline", engine=self.engine)
        pipeline.run(self.dlt_resource(data))
