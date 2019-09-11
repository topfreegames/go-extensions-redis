package redis

import (
	"context"
	"fmt"

	goredis "github.com/go-redis/redis"
	"github.com/opentracing/opentracing-go"
	tracing "github.com/topfreegames/go-extensions-tracing"
)

// instrument adds open tracing instrumentation on a *client
func instrument(client *BaseClient) {
	client.Client.WrapProcess(makeMiddleware(client.Client))
	client.Client.WrapProcessPipeline(makeMiddlewarePipe(client.Client))
}

func makeMiddleware(
	client *goredis.Client,
) func(old func(cmd goredis.Cmder) error) func(cmd goredis.Cmder) error {
	return func(old func(cmd goredis.Cmder) error) func(cmd goredis.Cmder) error {
		return func(cmd goredis.Cmder) error {
			tags := opentracing.Tags{
				"db.instance":  client.Options().DB,
				"db.statement": parseLong(cmd),
				"db.type":      "redis",
				"span.kind":    "client",
			}
			return trace(client.Context(), fmt.Sprintf("redis %s", cmd.Name()), tags, func() error {
				return old(cmd)
			})
		}
	}
}

func makeMiddlewarePipe(
	client *goredis.Client,
) func(old func(cmds []goredis.Cmder) error) func(cmds []goredis.Cmder) error {
	return func(old func(cmds []goredis.Cmder) error) func(cmds []goredis.Cmder) error {
		return func(cmds []goredis.Cmder) error {
			statement := ""
			for idx, cmd := range cmds {
				if idx > 0 {
					statement = statement + "\n" + parseLong(cmd)
				} else {
					statement = parseLong(cmd)
				}
			}
			tags := opentracing.Tags{
				"db.instance":  client.Options().DB,
				"db.statement": statement,
				"db.type":      "redis",
				"span.kind":    "client",
			}
			return trace(client.Context(), "redis pipe", tags, func() error {
				return old(cmds)
			})
		}
	}
}

func trace(ctx context.Context, operationName string, tags opentracing.Tags, f func() error) error {
	if ctx == nil {
		return f()
	}
	var parent opentracing.SpanContext
	if span := opentracing.SpanFromContext(ctx); span != nil {
		parent = span.Context()
	}
	reference := opentracing.ChildOf(parent)
	span := opentracing.StartSpan(operationName, reference, tags)
	defer span.Finish()
	defer tracing.LogPanic(span)
	err := f()
	if err != nil {
		message := err.Error()
		tracing.LogError(span, message)
	}
	return err
}

func parseLong(cmd goredis.Cmder) string {
	stmt := cmd.Name()
	for _, arg := range cmd.Args() {
		stmt = fmt.Sprintf("%s %v", stmt, arg)
	}
	return stmt
}
