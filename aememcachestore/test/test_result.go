package main

type TestResult struct {
	Name     string
	OK       bool
	Messages []string
}

func NewTestResult(name string) *TestResult {
	return &TestResult{Name: name, OK: true, Messages: []string{}}
}

func (r *TestResult) AddMessage(msg string) {
	r.Messages = append(r.Messages, msg)
}

func (r *TestResult) AddError(err error) {
	r.OK = false
	r.Messages = append(r.Messages, err.Error())
}

func (r *TestResult) Html() string {
	var color string
	var okng string
	if r.OK {
		color = "green"
		okng = "OK"
	} else {
		color = "red"
		okng = "ERROR"
	}
	html := "<li style=\"color: " + color + ";\">" + r.Name + "&nbsp;" + okng
	if len(r.Messages) > 0 {
		html += "<ul>"
		for _, m := range r.Messages {
			html += "<li>" + m + "</li>"
		}
		html += "</ul>"
	}
	html += "</li>"
	return html
}
