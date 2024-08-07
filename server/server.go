package server

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/sudo-nick16/nord/rafter"
)

func Logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println(r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

func NewServer(rafter *rafter.Rafter) http.Server {
	r := http.NewServeMux()

	r.HandleFunc("GET /{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		log.Printf("key : %s", key)
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		val, err := rafter.Get([]byte(key))
		if err == nil {
			b, err := json.Marshal(map[string]string{
				key: string(val),
			})
			if err != nil {
				log.Printf("get /key: json(kv) - %+v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Write(b)
			return
		}
		log.Printf("get /key: error - %+v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	})

	r.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		m := map[string]string{}
		err := json.NewDecoder(r.Body).Decode(&m)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		for k, v := range m {
			err := rafter.Put([]byte(k), []byte(v))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
	})

	r.HandleFunc("DELETE /{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		rafter.Delete([]byte(key))
	})

	r.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		keys := rafter.ListKeys()
		b, err := json.Marshal(map[string]interface{}{
			"keys": keys,
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, err = w.Write(b)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	})

	r.HandleFunc("GET /join", func(w http.ResponseWriter, r *http.Request) {
		m := make(map[string]string)
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(m) != 2 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		remoteAddr, ok := m["addr"]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		nodeId, ok := m["id"]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := rafter.Join(nodeId, remoteAddr); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	})

	s := http.Server{
		Addr:    ":8080",
		Handler: Logger(r),
	}

	return s
}
