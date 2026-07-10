package main

import (
	"log"
	"net/http"
	"os"
	"reflect"

	"github.com/samber/lo"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // ローカル開発用のデフォルト
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		rs, err := Test()
		if err != nil {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(ErrorHtml(err)))
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(ResultHtml(rs)))
	})

	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func ErrorHtml(err error) string {
	return "<div style='color:red;'>" + err.Error() + "</div>"
}

func ResultHtml(rs []*TestResult) string {
	html := lo.Reduce(rs, func(acc string, r *TestResult, _ int) string {
		return acc + r.Html()
	}, "")
	return "<html><head><title>test</title></head><body>" + html + "</body></html>"
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
