package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"local-search/internal/api"
	"local-search/internal/config"
	"local-search/internal/service"
	"local-search/internal/watcher"
)

// Run собирает сервис, строит индекс, поднимает HTTP и watcher, потом гасит всё по сигналу.
func Run(parent context.Context, cfg config.Config) error {
	// Делаем контекст, который отменяется по Ctrl+C или SIGTERM.
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	// В конце снимаем слушатель сигналов.
	defer stop()

	// Создаём сервис: сканер + индекс Bleve.
	svc, err := service.New(cfg)
	if err != nil {
		// Сервис не создался — здесь ещё нечего закрывать, просто возвращаем ошибку.
		return err
	}
	// При любом выходе из Run закрываем индекс.
	defer func() {
		if err := svc.Close(); err != nil {
			slog.Warn("оффнули сервис", "error", err)
		}
	}()

	// Watcher подписывается до обхода. События во время индексации копятся и применяются после неё.
	ready := make(chan struct{}) // канал для сигнала о том, что индексирование завершено
	startErr := make(chan error, 1) // канал для ошибок при запуске watcher
	go func() {
		w := watcher.New(cfg, svc)
		err := w.Run(ctx, func(ctx context.Context) error {
			if err := svc.BuildInitialIndex(ctx); err != nil {
				return err
			}
			slog.Info("Индексирование готово", "stats", svc.Stats())
			close(ready)
			return nil
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("watcher failed", "error", err)
			stop()
		}
		select {
		case startErr <- err:
		default:
		}
	}()

	select {
	case err := <-startErr:
		return err
	case <-ready:
	}

	// Собираем HTTP-сервер, порт ещё не слушаем.
	httpServer := newHTTPServer(cfg, svc)
	// Запускаем HTTP в отдельной горутине, чтобы не заблокировать Run.
	go serveHTTP(ctx, httpServer, stop)

	// Ждём, пока контекст не отменится. Дальше не идём, пока сервис работает.
	<-ctx.Done()
	slog.Info("Старт выключения")

	// Для Shutdown берём свежий контекст на 10 секунд: старый ctx уже отменён.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Перестаём принимать новые запросы и ждём текущие.
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Warn("http выключение с ошибкой", "error", err)
	}
	slog.Info("Выключение завершено")
	return nil
}

// newHTTPServer собирает стандартный http.Server с нашими ручками.
func newHTTPServer(cfg config.Config, svc *service.Service) *http.Server {
	apiServer := api.New(cfg, svc)
	return &http.Server{
		Addr:              cfg.Address,         // слушаем адрес из конфига
		Handler:           apiServer.Handler(), // наши маршруты /search, /stats, /docs/
		ReadHeaderTimeout: 5 * time.Second,     // обрываем слишком долгие заголовки
	}
}

// serveHTTP слушает порт, пока сервер жив.
func serveHTTP(ctx context.Context, httpServer *http.Server, stop context.CancelFunc) {
	slog.Info("http начал работу", "address", httpServer.Addr)
	// ListenAndServe блокируется, пока сервер не закроют.
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		// ErrServerClosed — штатное закрытие, его не считаем аварией.
		slog.Error("http сломался", "error", err)
		// Отменяем общий контекст, чтобы выключить watcher и Run.
		stop()
	}
	// Держим горутину до общей остановки.
	<-ctx.Done()
}
