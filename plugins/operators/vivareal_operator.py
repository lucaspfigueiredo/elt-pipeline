import json
import logging

from hooks.vivareal_hook import VivarealHook
from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class VivarealOperator(BaseOperator):
        
    template_fields = [
        "s3_key",
        "s3_bucket_name"
    ]
    
    @apply_defaults
    def __init__(self, s3_key, s3_bucket_name, *args, **kwargs) -> None:        
        super().__init__(*args, **kwargs)
        self.s3_key = s3_key
        self.s3_bucket_name = s3_bucket_name
    
    def execute(self, context):
        hook = VivarealHook()
        s3_hook = S3Hook(aws_conn_id="s3_connection")
        logger.info(f"Getting data")
        with open("vivareal.json", "w") as fp:
            for blocks in hook.run():
                for ap in blocks:
                    json.dump(ap, fp, ensure_ascii=False)
                    fp.write(",\n")
            logger.info(f"Uploading object in S3 {self.s3_bucket_name}")
            s3_hook.load_file(
                filename=fp.name,
                key=self.s3_key,
                bucket_name=self.s3_bucket_name
            )

if __name__ == "__main__":
    operator = VivarealOperator(file_path="/some/directory")
    operator.execute()