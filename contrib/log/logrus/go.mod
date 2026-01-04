module github.com/go-kratos/kratos/contrib/log/logrus/v2

go 1.23.0

require (
	github.com/go-kratos/kratos/v2 v2.9.2
	github.com/sirupsen/logrus v1.8.1
)

require golang.org/x/sys v0.32.0 // indirect

replace github.com/go-kratos/kratos/v2 => ../../../
