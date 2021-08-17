package bloom

type Stage int

const (
	GlobalInit Stage = iota + 1
	LoadData   Stage = iota + 1
)

type HooksInteraction interface {
	Before(args ...interface{})
	AfterSuccess(args ...interface{})
	AfterFail(err error, args ...interface{})
}

type Hook struct {
	Stage          Stage
	BeforeFn       func(args ...interface{})
	AfterSuccessFn func(args ...interface{})
	AfterFailFn    func(err error, args ...interface{})
}

func (h *Hook) Before(args ...interface{}) {
	if h.BeforeFn != nil {
		h.BeforeFn(args...)
	}
}

func (h *Hook) AfterSuccess(args ...interface{}) {
	if h.AfterSuccessFn != nil {
		h.AfterSuccessFn(args...)
	}
}

func (h *Hook) AfterFail(err error, args ...interface{}) {
	if h.AfterFailFn != nil {
		h.AfterFailFn(err, args...)
	}
}

type Hooks struct {
	hooks map[Stage]HooksInteraction
}

func (hs *Hooks) Before(stage Stage, args ...interface{}) {
	hs.getHook(stage).Before(args...)
}

func (hs *Hooks) AfterSuccess(stage Stage, args ...interface{}) {
	hs.getHook(stage).AfterSuccess(args...)
}

func (hs *Hooks) AfterFail(stage Stage, err error, args ...interface{}) {
	hs.getHook(stage).AfterFail(err, args...)
}

func (hs *Hooks) getHook(stage Stage) HooksInteraction {
	if h, exists := hs.hooks[stage]; exists {
		return h
	}
	return noOpHookInst
}

var noOpHookInst = noOpHook{}

type noOpHook struct {
}

func (n noOpHook) Before(args ...interface{}) {}

func (n noOpHook) AfterSuccess(args ...interface{}) {}

func (n noOpHook) AfterFail(err error, args ...interface{}) {}

var _ HooksInteraction = &Hook{}
var _ HooksInteraction = noOpHook{}
