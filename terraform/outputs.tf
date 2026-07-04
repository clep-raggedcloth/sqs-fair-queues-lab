output "experiment_config" {
  description = "Configuration consumed by the Go experiment runner."
  value = {
    region = var.aws_region
    scenarios = {
      for name, scenario in local.scenarios : name => {
        queue_url            = aws_sqs_queue.experiment[name].url
        queue_name           = aws_sqs_queue.experiment[name].name
        log_group            = aws_cloudwatch_log_group.consumer[name].name
        function_name        = aws_lambda_function.consumer[name].function_name
        use_message_group_id = scenario.use_message_group_id
        maximum_concurrency  = scenario.maximum_concurrency
      }
    }
  }
}

output "dashboard_name" {
  value = aws_cloudwatch_dashboard.experiment.dashboard_name
}

output "reserved_concurrency_total" {
  description = "Total reserved concurrency requested when reserve_concurrency is true."
  value       = var.reserve_concurrency ? sum([for scenario in values(local.scenarios) : scenario.maximum_concurrency + 5]) : 0
}
