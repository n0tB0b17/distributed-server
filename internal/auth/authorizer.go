package auth

import (
	"fmt"

	"github.com/casbin/casbin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
}

func New(model, policy string) *Authorizer {
	enforcer := casbin.NewEnforcer(model, policy)
	return &Authorizer{
		enforcer: enforcer,
	}
}

func (A *Authorizer) Authorize(subject, object, action string) error {
	resp := A.enforcer.Enforce(subject, action, object)
	if !resp {
		msg := fmt.Sprintf(
			"%s %s %s",
			subject,
			action,
			object,
		)

		st := status.New(codes.PermissionDenied, msg)
		return st.Err()
	}

	return nil
}
