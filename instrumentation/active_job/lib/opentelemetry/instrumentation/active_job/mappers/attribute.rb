# frozen_string_literal: true

# Copyright The OpenTelemetry Authors
#
# SPDX-License-Identifier: Apache-2.0

module OpenTelemetry
  module Instrumentation
    module ActiveJob
      module Mappers
        # Maps ActiveJob Attributes to Semantic Conventions
        class Attribute
          # Generates a set of attributes to add to a span using
          # general and messaging semantic conventions as well as
          # using `rails.active_job.*` namespace for custom attributes
          #
          # @param payload [Hash] of an ActiveSupport::Notifications payload
          # @return [Hash<String, Object>] of semantic attributes
          def call(payload)
            job = payload.fetch(:job)

            otel_attributes = {
              'code.namespace' => job.class.name,
              'messaging.system' => 'active_job',
              'messaging.destination' => job.queue_name,
              'messaging.message.id' => job.job_id,
              'messaging.active_job.adapter.name' => job.class.queue_adapter_name
            }

            otel_attributes['com.joyful_programming.messaging.message.latency'] = latency_of(job) if enqueued?(job)

            # Not all adapters generate or provide back end specific ids for messages
            otel_attributes['messaging.active_job.message.provider_job_id'] = job.provider_job_id.to_s if job.provider_job_id
            # This can be problematic if programs use invalid attribute types like Symbols for priority instead of using Integers.
            otel_attributes['messaging.active_job.message.priority'] = job.priority.to_s if job.priority

            otel_attributes.compact!

            otel_attributes
          end

          def start(payload)
            call(payload)
          end

          def finish(payload)
            job = payload.fetch(:job)

            otel_attributes = {
              'com.joyful_programming.messaging.message.execution_count' => job.executions
            }

            otel_attributes.compact!

            otel_attributes
          end

          private

          def enqueued?(job)
            job.respond_to?(:enqueued_at) && job.enqueued_at
          end

          def latency_of(job)
            1000 * (Time.now.to_f - Time.parse(job.enqueued_at.to_s).to_f)
          end
        end
      end
    end
  end
end
