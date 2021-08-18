package bloom

type Stage int

const (
	Default Stage = iota
	GlobalInit
	RedisInit
	LoadData
	StartUpdatesListening
	GenerateBuckets
	GenerateParticularBucket
	ApplySources
	ApplyParticularBucketSource
	DumpStateInRedis
	FinalizeFilter
	FinalizeParticularBucketFilter
	RedisFiltersStateCheck
	RedisParticularBucketStateCheck
	BulkLoadingFromRedis
	BulkLoadingFromRedisForParticularBucket
)

type Hook interface {
	GetStage() Stage
	Before(args ...interface{})
	After(optionalErr error, args ...interface{})
	AfterSuccess(args ...interface{})
	AfterFail(err error, args ...interface{})
}

type HookImpl struct {
	Stage          Stage
	BeforeFn       func(args ...interface{})
	AfterSuccessFn func(args ...interface{})
	AfterFailFn    func(err error, args ...interface{})
}

func (h *HookImpl) GetStage() Stage {
	return h.Stage
}

func (h *HookImpl) Before(args ...interface{}) {
	if h.BeforeFn != nil {
		h.BeforeFn(args...)
	}
}

func (h *HookImpl) After(optionalErr error, args ...interface{}) {
	if optionalErr != nil {
		h.AfterFail(optionalErr, args...)
	} else {
		h.AfterSuccess(args...)
	}
	if h.AfterSuccessFn != nil {
		h.AfterSuccessFn(args...)
	}
}

func (h *HookImpl) AfterSuccess(args ...interface{}) {
	if h.AfterSuccessFn != nil {
		h.AfterSuccessFn(args...)
	}
}

func (h *HookImpl) AfterFail(err error, args ...interface{}) {
	if h.AfterFailFn != nil {
		h.AfterFailFn(err, args...)
	}
}

type Hooks struct {
	hooks       map[Stage]Hook
	defaultHook Hook
}

func NewHooks(hooks ...Hook) *Hooks {
	return NewHooksWithDefault(noOpHookInst, hooks...)
}

func NewHooksWithDefault(defaultHook Hook, hooks ...Hook) *Hooks {
	hs := &Hooks{
		hooks:       make(map[Stage]Hook, len(hooks)),
		defaultHook: defaultHook,
	}
	for _, h := range hooks {
		hs.hooks[h.GetStage()] = h
	}
	return hs
}

func (hs *Hooks) Before(stage Stage, args ...interface{}) {
	hs.getHook(stage).Before(args...)
}

func (hs *Hooks) After(stage Stage, optionalErr error, args ...interface{}) {
	hs.getHook(stage).After(optionalErr, args...)
}

func (hs *Hooks) AfterSuccess(stage Stage, args ...interface{}) {
	hs.getHook(stage).AfterSuccess(args...)
}

func (hs *Hooks) AfterFail(stage Stage, err error, args ...interface{}) {
	hs.getHook(stage).AfterFail(err, args...)
}

func (hs *Hooks) getHook(stage Stage) Hook {
	if h, exists := hs.hooks[stage]; exists {
		return h
	}
	return noOpHookInst
}

var noOpHookInst = noOpHook{}

type noOpHook struct {
}

func (n noOpHook) GetStage() Stage {
	return Default
}

func (n noOpHook) Before(args ...interface{}) {}

func (n noOpHook) After(optionalErr error, args ...interface{}) {}

func (n noOpHook) AfterSuccess(args ...interface{}) {}

func (n noOpHook) AfterFail(err error, args ...interface{}) {}

var _ Hook = &HookImpl{}
var _ Hook = noOpHook{}
