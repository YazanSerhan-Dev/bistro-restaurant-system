package Server;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;
/**
 * JavaFX entry point for the Bistro Server application.
 *
 * <p>
 * This class is responsible only for:
 * <ul>
 *   <li>Loading the ServerView.fxml file</li>
 *   <li>Creating the main JavaFX window (Stage)</li>
 *   <li>Launching the JavaFX application lifecycle</li>
 * </ul>
 *
 * <p>
 * All server logic (start/stop, client connections, logging)
 * is handled by {@link ServerController} and {@link BistroServer}.
 */
public class ServerFX extends Application {

    private ServerController controller;

    /**
     * Called automatically by the JavaFX runtime after launch().
     * Initializes and shows the main server window.
     *
     * @param primaryStage the main application window
     */
    @Override
    public void start(Stage primaryStage) throws Exception {
        FXMLLoader loader = new FXMLLoader(getClass().getResource("ServerView.fxml"));
        Parent root = loader.load();
        controller = loader.getController();

        primaryStage.setTitle("Bistro Server - listening for client connections");
        primaryStage.setScene(new Scene(root));
        primaryStage.show();
    }

    /**
     * Called by JavaFX when the application exits (window closed with X, or Platform.exit()).
     * Stops the server so the process really ends and the port is released.
     */
    @Override
    public void stop() {
        if (controller != null) controller.shutdown();
    }

    /**
     * Application entry point.
     * Starts the JavaFX runtime, which then calls {@link #start(Stage)}.
     */
    public static void main(String[] args) {
        ServerConfig.applyTimeZone(); // must happen before any date/time work
        launch(args);
    }
}




