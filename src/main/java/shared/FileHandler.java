package shared;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileHandler {
    private String serializedObj = null;
    private final String stationID;
    public final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    // meta data
    private int lamportClock;
    private long lastUpdated;          // epoch millis
    private int globalUpdateCount;     // total updates across system at time of writing
    private String lastHost;           // host that sent the update
    private int lastPort;              // port that sent the update

    private boolean safeWrite(int lamportClock, long lastUpdated, int globalUpdateCount, String lastHost, int lastPort, String serializedObj) {
        String tempFileName = stationID + "-temp.json";
        String mainFileName = stationID + ".json";

        File tempFile = new File(tempFileName);
        File mainFile = new File(mainFileName);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFileName))) {
            // Build JSON object
            writer.write("{");
            writer.newLine();

            writer.write("  \"meta\": {");
            writer.newLine();
            writer.write("    \"lamport\": " + lamportClock + ",");
            writer.newLine();
            writer.write("    \"lastUpdated\": " + lastUpdated + ",");
            writer.newLine();
            writer.write("    \"updateCount\": " + globalUpdateCount + ",");
            writer.newLine();
            writer.write("    \"host\": \"" + lastHost + "\",");
            writer.newLine();
            writer.write("    \"port\": " + lastPort);
            writer.newLine();
            writer.write("  },");
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

    public void writeToFile(String serializedObj, int lamportClock, long lastUpdated, int globalUpdateCount, String lastHost, int lastPort) {
        rwLock.writeLock().lock();
        try {
            if (this.lamportClock < lamportClock) {
                if (!safeWrite(lamportClock, lastUpdated, globalUpdateCount, lastHost, lastPort, serializedObj)) {
                    return;
                }
                this.lamportClock = lamportClock;
                this.serializedObj = serializedObj;
                this.lastUpdated = System.currentTimeMillis();
                this.globalUpdateCount = globalUpdateCount;
                this.lastHost = lastHost;
                this.lastPort = lastPort;
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

            // Extract metadata
            JsonObject meta = json.getAsJsonObject("meta");
            this.lamportClock = meta.get("lamport").getAsInt();
            this.lastUpdated = meta.get("lastUpdated").getAsLong();
            this.globalUpdateCount = meta.get("updateCount").getAsInt();
            this.lastHost = meta.get("host").getAsString();
            this.lastPort = meta.get("port").getAsInt();

            // Extract body (as JSON string)
            String body = json.get("body").toString();
            this.serializedObj = body;

        } catch (IOException | JsonSyntaxException e) {
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

    public long getLastUpdated() {
        rwLock.readLock().lock();
        try {
            return lastUpdated;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public int getGlobalUpdateCount() {
        rwLock.readLock().lock();
        try {
            return globalUpdateCount;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public String getLastHost() {
        rwLock.readLock().lock();
        try {
            return lastHost;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public int getLastPort() {
        rwLock.readLock().lock();
        try {
            return lastPort;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public boolean isExpired(int currentGlobalUpdateCount) {
        rwLock.readLock().lock();
        try {
            long ageMs = System.currentTimeMillis() - lastUpdated;
            if (ageMs > 30_000) return true;
            return (currentGlobalUpdateCount - globalUpdateCount > 20);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void deleteFileFromDisk() {
        try {
            Path mainFile = Paths.get(stationID + ".json");
            Files.deleteIfExists(mainFile);
        } catch (IOException e) {
            System.err.println("Failed to delete file: " + e.getMessage());
        }
    }
}
