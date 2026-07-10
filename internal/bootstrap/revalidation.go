package bootstrap

import (
	"broadcasting/internal/domain/notification/actions"
	"broadcasting/internal/infrastructure/app"
	"broadcasting/internal/infrastructure/jobs"
	grpcprovider "broadcasting/internal/infrastructure/providers/grpc"
	"context"
)

/*
startRevalidationJob wires the token revalidation job: a gRPC client to auth
plus a ticker goroutine that closes stale WebSocket connections. It runs in
this process because it iterates the Hub's in-memory connections. The caller
owns the lifecycle: canceling ctx stops the ticker loop, and the client is
registered as a closer, so CloseAll releases the gRPC connection.
*/
func startRevalidationJob(ctx context.Context, appInstance *app.App) error {
	authClient, err := grpcprovider.NewAuthClient(
		appInstance.Config.Auth.GRPCAddress,
		appInstance.Config.Auth.RevalidationTimeout,
	)

	if err != nil {
		return err
	}

	appInstance.AddCloser(authClient.Close)

	revalidateAction := actions.NewRevalidateTokens(appInstance.Container.Hub, authClient)
	go jobs.Run(ctx, "token_revalidation", appInstance.Config.Auth.RevalidationInterval, revalidateAction.Execute)

	return nil
}
