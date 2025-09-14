package shared;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileHandler {
    private int lamportClock;
    private String serializedObj = null;
    private final String stationID;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    private boolean safeWrite(int lamportClock, String serializedObj) {
        String tempFileName = stationID + "-temp.json";
        String mainFileName = stationID + ".json";

        File tempFile = new File(tempFileName);
        File mainFile = new File(mainFileName);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFileName))) {
            // Build JSON object
            writer.write("{");
            writer.newLine();

            // Lamport clock field
            writer.write("  \"lamport\": " + lamportClock + ",");
            writer.newLine();

            // Body field (already serialized JSON string)
            writer.write("  \"body\": " + serializedObj);
            writer.newLine();

            writer.write("}");
        } catch (IOException e) {
            System.err.println("Failed to write temp file: " + e.getMessage());
            return false;
        }

        try {
            // Atomic replace: temp -> main
            Files.move(Paths.get(tempFileName),
                    Paths.get(mainFileName),
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            System.err.println("Failed to replace main file: " + e.getMessage());
            return false;
        }

        System.out.println("Successfully wrote to " + mainFileName);
        return true;
    }

    public FileHandler(String stationID) {
        this.stationID = stationID;
        this.lamportClock = 0;
    }

    public void writeToFile(String serializedObj, int lamportClock) {
        rwLock.writeLock().lock();
        try {
            if (this.lamportClock < lamportClock) {
                if (!safeWrite(lamportClock, serializedObj)) {
                    return;
                }
                this.lamportClock = lamportClock;
                this.serializedObj = serializedObj;
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public String getSerializedObj() {
        rwLock.readLock().lock();
        try {
            return this.serializedObj;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void readFromFile(String stationID) {

        String fileName = stationID + ".json";

        try (FileReader reader = new FileReader(fileName)) {
            Gson gson = new Gson();

            // Parse file into a JsonObject
            JsonObject json = gson.fromJson(reader, JsonObject.class);

            // Extract lamport clock
            int lamport = json.get("lamport").getAsInt();

            // Extract body (as JSON string)
            String body = json.get("body").toString();

            this.lamportClock = lamport;
            this.serializedObj = body;

        } catch (IOException | JsonSyntaxException e ) {
            System.err.println("Failed to read file: " + e.getMessage());
        }
    }

    public int getLamportClock() {
        rwLock.readLock().lock();
        try {
            return lamportClock;
        } finally {
            rwLock.readLock().unlock();
        }
    }
}
