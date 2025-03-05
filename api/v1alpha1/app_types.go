package v1alpha1

import (
	"context"
	"go.uber.org/zap"
)

// Context TODO
type Application struct {
	Config  *ConfigSpec
	Logger  *zap.Logger
	Context context.Context
}
