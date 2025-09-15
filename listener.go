package rdsd

type BaseListener struct {
	watchNames []string
}

func NewBaseListener(watchNames ...string) *BaseListener {
	return &BaseListener{watchNames: watchNames}
}

func (l BaseListener) WatchNames() (names []string) {
	return l.watchNames
}

func (BaseListener) OnAdd(info ServerInfo) {
}

func (BaseListener) OnRemove(info ServerInfo) {
}

func (BaseListener) OnUpdate(info ServerInfo) {
}
