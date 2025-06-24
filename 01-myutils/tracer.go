package myutils

import (
	jaeger "github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

func SetupTracer(serviceName string) (func(), error) {

	// Jaeger setup code
	//
	config := jaegercfg.Configuration{
		Sampler: &jaegercfg.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
	}

	closer, err := config.InitGlobalTracer(serviceName)
	if err != nil {
		return nil, err
	}

	cleanupFunc := func() {
		closer.Close()
	}

	return cleanupFunc, nil
}
