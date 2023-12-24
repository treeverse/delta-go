package action

type SetTransaction struct {
	AppId       string `json:"appId"`
	Version     int64  `json:"version"`
	LastUpdated *int64 `json:"lastUpdated,omitempty"`
}

func (s *SetTransaction) Wrap() *SingleAction {
	return &SingleAction{Txn: s}
}

func (s *SetTransaction) Json() (string, error) {
	return jsonString(s)
}
