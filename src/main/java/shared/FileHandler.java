package shared;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The {@code FileHandler} is responsible for persisting and managing station data
 * in JSON files on disk.
 *
 * <p>Features:</p>
 * <ul>
 *   <li>Atomic writes using a temporary file and that replaces the main file.</li>
 *   <li>Stores metadata such as Lamport clock, last update time, global update count, and sender info.</li>
 *   <li>Thread safety guaranteed using a {@link ReentrantReadWriteLock}.</li>
 *   <li>Supports reading persisted files back into memory on server restart.</li>
 *   <li>Expiration checks based on elapsed time or global update count differences.</li>
 * </ul>
 */
public class FileHandler {
    private String serializedObj = null;
    private final String stationID;
    public final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    // Metadata fields
    private int lamportClock;
    private long lastUpdated;          // Epoch millis
    private int globalUpdateCount;     // Total updates across system at time of writing
    private String lastHost;           // Host that sent the update
    private int lastPort;              // Port that sent the update

    /**
     * Performs an atomic write of station data to disk using a temp file.
     *
     * @param lamportClock      the Lamport clock value
     * @param lastUpdated       epoch time of the update
     * @param globalUpdateCount global update count at the time of writing
     * @param lastHost          host that submitted the update
     * @param lastPort          port that submitted the update
     * @param serializedObj     the serialized JSON body of the station entry
     * @return {@code true} if write succeeded, {@code false} otherwise
     */
    private boolean safeWrite(int lamportClock, long lastUpdated,
                              int globalUpdateCount, String lastHost,
                              int lastPort, String serializedObj) {
        String tempFileName = stationID + "-temp.json";
        String mainFileName = stationID + ".json";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFileName))) {
            // Build JSON structure with meta + body
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

    /**
     * Constructs a new FileHandler for the given station ID.
     *
     * @param stationID the unique station identifier
     */
    public FileHandler(String stationID) {
        this.stationID = stationID;
        this.lamportClock = 0;
    }

    /**
     * Writes a new update to the station file if the Lamport clock is newer.
     *
     * <p>This method acquires a write lock and performs an atomic disk write.
     * Metadata and serialized body are updated in memory after a successful write.</p>
     *
     * @param serializedObj     serialized JSON body of the station entry
     * @param lamportClock      Lamport clock of the incoming update
     * @param lastUpdated       epoch millis of the update
     * @param globalUpdateCount global update count at the time of writing
     * @param lastHost          host that submitted the update
     * @param lastPort          port that submitted the update
     */
    public void writeToFile(String serializedObj, int lamportClock, long lastUpdated,
                            int globalUpdateCount, String lastHost, int lastPort) {
        rwLock.writeLock().lock();
        try {
            if (this.lamportClock < lamportClock) {
                // if write was successful
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

    /**
     * Returns the serialized body of the station entry.
     *
     * @return JSON string body
     */
    public String getSerializedObj() {
        rwLock.readLock().lock();
        try {
            return this.serializedObj;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Reads a persisted JSON file from disk and loads its metadata and body into memory.
     *
     * @param stationID the station identifier
     */
    public void readFromFile(String stationID) {
        String fileName = stationID + ".json";

        try (FileReader reader = new FileReader(fileName)) {
            Gson gson = new Gson();
            JsonObject json = gson.fromJson(reader, JsonObject.class);

            JsonObject meta = json.getAsJsonObject("meta");
            this.lamportClock = meta.get("lamport").getAsInt();
            this.lastUpdated = meta.get("lastUpdated").getAsLong();
            this.globalUpdateCount = meta.get("updateCount").getAsInt();
            this.lastHost = meta.get("host").getAsString();
            this.lastPort = meta.get("port").getAsInt();

            this.serializedObj = json.get("body").toString();
        } catch (IOException | JsonSyntaxException e) {
            System.err.println("Failed to read file: " + e.getMessage());
        }
    }

    /** @return the Lamport clock value stored for this station */
    public int getLamportClock() {
        rwLock.readLock().lock();
        try {
            return lamportClock;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /** @return the last update timestamp in epoch millis */
    public long getLastUpdated() {
        rwLock.readLock().lock();
        try {
            return lastUpdated;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /** @return the global update count when this entry was last written */
    public int getGlobalUpdateCount() {
        rwLock.readLock().lock();
        try {
            return globalUpdateCount;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /** @return the host that submitted the last update */
    public String getLastHost() {
        rwLock.readLock().lock();
        try {
            return lastHost;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /** @return the port that submitted the last update */
    public int getLastPort() {
        rwLock.readLock().lock();
        try {
            return lastPort;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Determines whether this file is expired.
     *
     * <p>A file is considered expired if:</p>
     * <ul>
     *   <li>Its age exceeds 30 seconds, OR</li>
     *   <li>The difference between the current global update count and the file’s update count is greater than 20.</li>
     * </ul>
     *
     * @param currentGlobalUpdateCount the latest known global update count
     * @return {@code true} if expired, {@code false} otherwise
     */
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

    /**
     * Deletes the station’s main JSON file from disk if it exists.
     */
    public void deleteFileFromDisk() {
        try {
            Path mainFile = Paths.get(stationID + ".json");
            Files.deleteIfExists(mainFile);
        } catch (IOException e) {
            System.err.println("Failed to delete file: " + e.getMessage());
        }
    }
}
