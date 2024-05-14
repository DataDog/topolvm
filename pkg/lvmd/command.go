package lvmd

import (
	internalLvmdCommand "github.com/topolvm/topolvm/internal/lvmd/command"
)

// Containerized sets whether to run lvm commands in a container.
var Containerized = internalLvmdCommand.Containerized
