package bloom

type testInterceptor interface {
	interfere(stage string)
}

type noOpInterceptor struct{}

func (n *noOpInterceptor) interfere(_ string) {
	// do nothing as it's an no-op interceptor
}

var defaultNoOp testInterceptor = &noOpInterceptor{}
