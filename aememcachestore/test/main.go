package main

import (
	"net/http"
	"os"
	"reflect"

	"github.com/labstack/echo/v4"
	"github.com/samber/lo"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // ローカル開発用のデフォルト
	}

	e := echo.New()
	e.GET("/", func(c echo.Context) error {
		rs, err := Test()
		if err != nil {
			return c.HTML(http.StatusInternalServerError, ErrorHtml(err))
		}
		return c.HTML(http.StatusOK, ResultHtml(rs))
	})
	e.Logger.Fatal(e.Start(":" + port))
}

func ErrorHtml(err error) string {
	return "<div style='color:red;'>" + err.Error() + "</div>"
}

func ResultHtml(rs []*TestResult) string {
	html := lo.Reduce(rs, func(acc string, r *TestResult, _ int) string {
		return acc + r.Html()
	}, "")
	return "<html><head><title>test</title></head></body>" + html + "</body></html>"
}

func Test() ([]*TestResult, error) {
	var results []*TestResult
	tests := &Tests{}
	val := reflect.ValueOf(tests)
	typ := reflect.TypeOf(tests)

	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i)
		if len(method.Name) >= 4 && method.Name[:4] == "Test" {
			res := val.Method(i).Call(nil)
			if len(res) == 1 {
				if tr, ok := res[0].Interface().(*TestResult); ok {
					results = append(results, tr)
				}
			}
		}
	}
	return results, nil
}
