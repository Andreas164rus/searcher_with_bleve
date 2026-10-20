package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"local-search/internal/app"
	"local-search/internal/config"
)

func main() {
	// Берём флажок -config, если его нет, используем config.json.
	configPath := flag.String("config", "config.json", "путь для json конфига")
	flag.Parse()

	// Читаем JSON-конфиг по указанному пути.
	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	if err := app.Run(context.Background(), cfg); err != nil {
		slog.Error("application stopped with error", "error", err)
		os.Exit(1)
	}
}
