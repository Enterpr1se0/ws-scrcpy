package contract

const (
	ActionListHosts      = "list-hosts"
	ActionGoogDeviceList = "goog-device-list"
	ActionMultiplex      = "multiplex"
	ActionShell          = "shell"
	ActionProxyWS        = "proxy-ws"
	ActionProxyADB       = "proxy-adb"
	ActionStream         = "stream"
	ActionFileListing    = "list-files"
)

const (
	ChannelFSLS = "FSLS"
	ChannelHSTS = "HSTS"
	ChannelSHEL = "SHEL"
	ChannelGTRC = "GTRC"
)

const (
	MessageTypeCreateChannel byte = 4
	MessageTypeCloseChannel  byte = 8
	MessageTypeRawBinaryData byte = 16
	MessageTypeRawStringData byte = 32
	MessageTypeData          byte = 64
)

const (
	CloseInvalidURL          = 4001
	CloseUnsupportedRequest  = 4002
	CloseInvalidParameter    = 4003
	CloseServiceStartFailed  = 4005
	CloseProxyUpstreamClosed = 4010
	CloseProxyUpstreamError  = 4011
	CloseShellExit           = 4500
)
