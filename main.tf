locals {
  package_filename = "${path.module}/package.zip"
}

data "external" "package" {
  program = ["bash", "-c", "curl -s -L -o ${local.package_filename} ${var.lambda_package_url} && echo {}"]
}

data "aws_s3_bucket" "failed_log_s3_bucket" {
  bucket = "${var.failed_log_s3_bucket}"
}

resource "aws_cloudwatch_log_group" "logs" {
  name = "/aws/lambda/${var.name}"

  retention_in_days = "${var.log_retention_in_days}"
}

resource "aws_lambda_function" "function" {
  function_name = "${var.name}"
  handler       = "${var.handler}"
  role          = "${module.iam.arn}"
  runtime       = "${var.runtime}"
  memory_size   = "${var.memory}"
  timeout       = "${var.timeout}"

  filename = "${local.package_filename}"

  # Below is a very dirty hack to force base64sha256 to wait until 
  # package download in data.external.package finishes.
  #
  # WARNING: explicit depends_on from this resource to data.external.package 
  # does not help

  //  source_code_hash = "${base64sha256(file("${jsonencode(data.external.package.result) == "{}" ? local.package_filename : ""}"))}"
  tracing_config {
    mode = "${var.tracing_mode}"
  }
  environment {
    variables {
      TZ = "${var.timezone}"

      LOG_ID_FIELD            = "${var.log_id_field}"
      LOG_TYPE_FIELD          = "${var.log_type_field}"
      LOG_TYPE_UNKNOWN_PREFIX = "${var.log_type_unknown_prefix}"
      LOG_TIMESTAMP_FIELD     = "${var.log_timestamp_field}"
      LOG_TYPE_WHITELIST      = "${join(",", var.log_type_field_whitelist)}"

      LOG_FLUENT_TARGET_URL = "${var.fluent_target_url}"
      LOG_FLUENT_TAG        = "${var.fluent_tag}"
      FAILED_LOG_S3_BUCKET  = "${var.failed_log_s3_bucket}"
      FAILED_LOG_S3_PREFIX  = "${var.failed_log_s3_prefix}"
    }
  }
  vpc_config {
    security_group_ids = [
      "${var.security_group_ids}",
    ]

    subnet_ids = [
      "${var.subnet_ids}",
    ]
  }
  tags = "${var.tags}"
}

resource "aws_lambda_event_source_mapping" "kinesis_mapping" {
  batch_size        = "${var.batch_size}"
  event_source_arn  = "${var.kinesis_stream_arn}"
  enabled           = true
  function_name     = "${aws_lambda_function.function.arn}"
  starting_position = "${var.starting_position}"
}

resource "aws_iam_role_policy_attachment" "xray_access" {
  policy_arn = "arn:aws:iam::aws:policy/AWSXrayWriteOnlyAccess"
  role       = "${module.iam.name}"
}

module "iam" {
  source  = "baikonur-oss/iam-nofile/aws"
  version = "v1.0.1"

  type = "lambda"
  name = "${var.name}"

  policy_json = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kinesis:DescribeStream",
                "kinesis:GetShardIterator",
                "kinesis:GetRecords",
                "kinesis:ListStreams"
            ],
            "Resource": [
                "${var.kinesis_stream_arn}"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogStream",
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:*:*:*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:CreateNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DeleteNetworkInterface"
            ],
            "Resource": [
                "*"
            ]
        }
    ]
}
EOF
}
