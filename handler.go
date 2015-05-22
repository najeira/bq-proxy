package main

import (
	"encoding/json"
	"fmt"
	"github.com/najeira/bigquery"
	"io/ioutil"
	"net/http"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type httpHandler struct {
	mu      sync.Mutex
	writers map[string]*bigquery.Writer
}

func newHttpHandler() *httpHandler {
	rand.Seed(time.Now().Nanosecond())
	return &httpHandler{
		writers: make(map[string]*bigquery.Writer),
	}
}

func (h *httpHandler) Close() {
	for _, writer := range h.writers {
		writer.Close()
	}
}

func (h *httpHandler) getBigqueryWriter(project, database, table string) (*bigquery.Writer, error) {
	key := fmt.Sprintf("%s|%s|%s", project, database, table)

	h.mu.Lock()
	defer h.mu.Unlock()

	writer, ok := h.writers[key]
	if ok {
		return writer, nil
	}

	writer, err := h.newBigqueryWriter(project, database, table)
	if err != nil {
		return nil, err
	}

	h.writers[key] = writer
	return writer, nil
}

func (h *httpHandler) newBigqueryWriter(project, database, table string) (*bigquery.Writer, error) {
	writer := bigquery.NewWriter(project, database, table)
	if err := writer.Connect(Options.Email, Options.Pem); err != nil {
		return nil, err
	}
	writer.SetLogger(logger)
	return writer, nil
}

func (h *httpHandler) internalError(w http.ResponseWriter, msg string) {
	logger.Infof(msg)
	w.WriteHeader(http.StatusInternalServerError)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte(`{"error": "` + msg + `"}`))
}

func (h *httpHandler) badRequest(w http.ResponseWriter, msg string) {
	logger.Infof(msg)
	w.WriteHeader(http.StatusBadRequest)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte(`{"error": "` + msg + `"}`))
}

func (h *httpHandler) ok(w http.ResponseWriter, msg []byte) {
	logger.Debugf(string(msg))
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(msg)
}

func (h *httpHandler) serveStatus(w http.ResponseWriter) {
}

func (h *httpHandler) sendLines(writer *bigquery.Writer, lines []string) []*writeError {
	var row map[string]interface{}
	errors := make([]*writeError, 0)
	for i, line := range lines {
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			errors = append(errors, &writeError{Index: i, Error: err})
			continue
		}
		if err := writer.Add(generateInsertId(10), row); err != nil {
			errors = append(errors, &writeError{Index: i, Error: err})
			continue
		}
	}
	return errors
}

func (h *httpHandler) serveBigquery(w http.ResponseWriter, project, dataset, table string, body []byte) {
	writer, err := h.getBigqueryWriter(project, dataset, table)
	if err != nil {
		h.internalError(w, err.Error())
		return
	}

	lines := strings.Split(string(body), "\n")
	errors := h.sendLines(writer, lines)

	resp, err := json.Marshal(&response{Errors: errors})
	if err != nil {
		h.internalError(w, err.Error())
		return
	}

	h.ok(w, resp)
}

func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		// top is status dashboard.
		h.serveStatus(w)
		return
	}

	params := strings.Split(r.URL.Path, "/")
	if len(params) != 4 {
		h.badRequest(w, "invalid uri")
		return
	}

	project := params[1]
	dataset := params[2]
	table := params[3]

	if project == "" || dataset == "" || table == "" {
		h.badRequest(w, "invalid uri")
		return
	}

	// read body
	body, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.badRequest(w, err.Error())
		return
	}

	h.serveBigquery(w, project, dataset, table, body)
}

type writeError struct {
	Index int   `json:index`
	Error error `json:error`
}

type response struct {
	Errors []*writeError `json:errors`
}

const characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func generateInsertId(length int) string {
	ret := make([]byte, length)
	for i := 0; i < length; i++ {
		ret[i] = characters[rand.Int()%len(characters)]
	}
	return string(ret)
}
