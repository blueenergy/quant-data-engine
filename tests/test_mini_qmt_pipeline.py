"""Test script for the miniQMT pipeline."""

import argparse
from quant_data_engine import create_mini_qmt_pipeline


def test_mini_qmt_pipeline():
    """Test the miniQMT pipeline creation."""
    print("Testing miniQMT Pipeline Creation")
    print("=" * 35)
    
    try:
        # Create the pipeline
        pipeline = create_mini_qmt_pipeline()
        print("✅ miniQMT pipeline created successfully")
        print(f"Pipeline type: {type(pipeline)}")
        print(f"Adapter type: {type(pipeline.adapter)}")
        print(f"Repository type: {type(pipeline.repository)}")
        
    except Exception as e:
        print(f"❌ Error creating miniQMT pipeline: {e}")
        import traceback
        traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(description="Test miniQMT pipeline")
    parser.add_argument("--test", action="store_true", help="Run pipeline test")
    
    args = parser.parse_args()
    
    if args.test:
        test_mini_qmt_pipeline()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()