#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    logger.info("Download input artifact from W&B: %s", args.input_artifact)
    local_path = wandb.use_artifact(args.input_artifact).file()
    df = pd.read_csv(local_path)

    logger.info("Dropping outliers for longitude and latitude")
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    logger.info("Dropping outliers for price column")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    logger.info("Format value of last_review column to datetime format")
    df['last_review'] = pd.to_datetime(df['last_review'])

    logger.info("Save cleaned data to csv file: %s", args.output_artifact)
    df.to_csv(args.output_artifact, index=False)

    logger.info("Upload saved data to W&B")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(args.output_artifact)
    run.log_artifact(artifact)
    run.finish()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Fully-qualified name for the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="output artifact name",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="output artifact type",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum value of price",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum value of price",
        required=True
    )


    args = parser.parse_args()

    go(args)
