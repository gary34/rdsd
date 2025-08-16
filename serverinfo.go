package rdsd

type BaseServerInfo struct {
	ID         string
	Name       string
	Version    string
	UpdateTime int64
}

func (b BaseServerInfo) GetID() string {
	return b.ID
}

func (b BaseServerInfo) GetName() string {
	return b.Name
}

func (b BaseServerInfo) GetUpdateTime() int64 {
	return b.UpdateTime
}

func (b BaseServerInfo) GetVersion() string {
	return b.Version
}

func (b BaseServerInfo) Clone() ServerInfo {
	return b
}
