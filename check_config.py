from nhl_pipeline.config import get_settings
import os
from dotenv import load_dotenv

# Load .env file if present (simulating local dev)
load_dotenv()

try:
    settings = get_settings()
    print("✅ Configuration Loaded Successfully!")
    print(f"  - AWS Region: {settings.aws_region}")
    print(f"  - S3 Bucket:  {settings.s3_bucket}")
    print(f"  - MWAA Bucket: {settings.mwaa_bucket}")
except Exception as e:
    print(f"❌ Configuration Failed: {e}")
