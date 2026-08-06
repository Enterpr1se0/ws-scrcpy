package web

import "embed"

// FS contains the webpack frontend assets under public/.
//
//go:embed all:public
var FS embed.FS
