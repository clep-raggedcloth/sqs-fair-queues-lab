locals {
  scenarios = {
    fair-c100 = {
      maximum_concurrency  = 100
      use_message_group_id = true
      description          = "Fair Queue enabled by tenant MessageGroupId; Lambda maximum concurrency 100"
    }
    baseline-c100 = {
      maximum_concurrency  = 100
      use_message_group_id = false
      description          = "Standard Queue baseline without MessageGroupId; Lambda maximum concurrency 100"
    }
    fair-c29 = {
      maximum_concurrency  = 29
      use_message_group_id = true
      description          = "Fair Queue enabled by tenant MessageGroupId; Lambda maximum concurrency 29"
    }
    baseline-c29 = {
      maximum_concurrency  = 29
      use_message_group_id = false
      description          = "Standard Queue baseline without MessageGroupId; Lambda maximum concurrency 29"
    }
    fair-c30 = {
      maximum_concurrency  = 30
      use_message_group_id = true
      description          = "Fair Queue enabled by tenant MessageGroupId; Lambda maximum concurrency 30"
    }
    baseline-c30 = {
      maximum_concurrency  = 30
      use_message_group_id = false
      description          = "Standard Queue baseline without MessageGroupId; Lambda maximum concurrency 30"
    }
  }

  lambda_zip = startswith(var.lambda_zip_path, "/") ? var.lambda_zip_path : "${path.module}/${var.lambda_zip_path}"
}

resource "aws_sqs_queue" "dlq" {
  for_each = local.scenarios

  name                      = "${var.project_name}-${each.key}-dlq"
  message_retention_seconds = 1209600

  tags = {
    Scenario = each.key
    Role     = "dead-letter-queue"
  }
}

resource "aws_sqs_queue" "experiment" {
  for_each = local.scenarios

  name                       = "${var.project_name}-${each.key}"
  visibility_timeout_seconds = var.lambda_timeout_seconds * 6
  message_retention_seconds  = 86400
  receive_wait_time_seconds  = 20

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq[each.key].arn
    maxReceiveCount     = 5
  })

  tags = {
    Scenario          = each.key
    FairQueueMessages = tostring(each.value.use_message_group_id)
    Description       = each.value.description
  }
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "consumer" {
  for_each = local.scenarios

  name               = "${var.project_name}-${each.key}-consumer"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "consumer" {
  for_each = local.scenarios

  statement {
    sid    = "ConsumeExperimentQueue"
    effect = "Allow"
    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ReceiveMessage",
    ]
    resources = [aws_sqs_queue.experiment[each.key].arn]
  }

  statement {
    sid    = "WriteFunctionLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["${aws_cloudwatch_log_group.consumer[each.key].arn}:*"]
  }
}

resource "aws_iam_role_policy" "consumer" {
  for_each = local.scenarios

  name   = "consume-and-log"
  role   = aws_iam_role.consumer[each.key].id
  policy = data.aws_iam_policy_document.consumer[each.key].json
}

resource "aws_cloudwatch_log_group" "consumer" {
  for_each = local.scenarios

  name              = "/aws/lambda/${var.project_name}-${each.key}-consumer"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "consumer" {
  for_each = local.scenarios

  function_name = "${var.project_name}-${each.key}-consumer"
  description   = each.value.description
  role          = aws_iam_role.consumer[each.key].arn
  runtime       = "provided.al2023"
  handler       = "bootstrap"
  architectures = ["arm64"]
  filename      = local.lambda_zip

  source_code_hash               = filebase64sha256(local.lambda_zip)
  memory_size                    = var.lambda_memory_size
  timeout                        = var.lambda_timeout_seconds
  reserved_concurrent_executions = var.reserve_concurrency ? each.value.maximum_concurrency + 5 : -1

  environment {
    variables = {
      SCENARIO = each.key
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.consumer,
    aws_iam_role_policy.consumer,
  ]
}

resource "aws_lambda_event_source_mapping" "consumer" {
  for_each = local.scenarios

  event_source_arn                   = aws_sqs_queue.experiment[each.key].arn
  function_name                      = aws_lambda_function.consumer[each.key].arn
  enabled                            = true
  batch_size                         = 1
  maximum_batching_window_in_seconds = 0

  scaling_config {
    maximum_concurrency = each.value.maximum_concurrency
  }
}

resource "aws_cloudwatch_query_definition" "message_starts" {
  for_each = local.scenarios

  name            = "${var.project_name}/${each.key}/message-starts"
  log_group_names = [aws_cloudwatch_log_group.consumer[each.key].name]
  query_string    = <<-QUERY
    fields @timestamp, experiment_id, tenant, phase, dwell_ms, work_ms
    | filter event_type = "message_started"
    | sort @timestamp asc
  QUERY
}

resource "aws_cloudwatch_dashboard" "experiment" {
  dashboard_name = "${var.project_name}-verification"

  dashboard_body = jsonencode({
    widgets = flatten([
      for index, scenario_name in sort(keys(local.scenarios)) : [
        {
          type   = "metric"
          x      = (index % 2) * 12
          y      = floor(index / 2) * 12
          width  = 12
          height = 6
          properties = {
            title  = "${scenario_name}: SQS state and noisy groups"
            region = var.aws_region
            period = 60
            stat   = "Maximum"
            metrics = [
              ["AWS/SQS", "ApproximateNumberOfNoisyGroups", "QueueName", aws_sqs_queue.experiment[scenario_name].name],
              ["AWS/SQS", "ApproximateNumberOfMessagesNotVisible", "QueueName", aws_sqs_queue.experiment[scenario_name].name],
              ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.experiment[scenario_name].name],
              ["AWS/SQS", "ApproximateNumberOfMessagesVisibleInQuietGroups", "QueueName", aws_sqs_queue.experiment[scenario_name].name],
            ]
          }
        },
        {
          type   = "metric"
          x      = (index % 2) * 12
          y      = floor(index / 2) * 12 + 6
          width  = 12
          height = 6
          properties = {
            title  = "${scenario_name}: Lambda concurrency"
            region = var.aws_region
            period = 60
            stat   = "Maximum"
            metrics = [
              ["AWS/Lambda", "ConcurrentExecutions", "FunctionName", aws_lambda_function.consumer[scenario_name].function_name],
              ["AWS/Lambda", "Errors", "FunctionName", aws_lambda_function.consumer[scenario_name].function_name],
              ["AWS/Lambda", "Throttles", "FunctionName", aws_lambda_function.consumer[scenario_name].function_name],
            ]
          }
        }
      ]
    ])
  })
}
