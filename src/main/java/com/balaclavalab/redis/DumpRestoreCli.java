package com.balaclavalab.redis;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.reactive.RedisKeyReactiveCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class DumpRestoreCli {

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("f", "uriFrom", true, "Redis from (e.g. redis://localhost/1)");
        options.addOption("t", "uriTo", true, "Redis to (e.g. redis://localhost/2)");
        options.addOption("m", "scanMatch", true, "Scan Match (default: *)");
        options.addOption("l", "scanLimit", true, "Scan Limit (default: 5000)");
        options.addOption("s", "SrcCluMod", true, "Source redis is in Cluster mode (default: no)");
        options.addOption("d", "DstCluMod", true, "Destination redis is in Cluster mode (default: no)");

        CommandLineParser commandLineParser = new DefaultParser();
        try {
            CommandLine commandLine = commandLineParser.parse(options, args);
            boolean hasOptions = commandLine.getOptions().length > 0;
            if (commandLine.hasOption("help") || !hasOptions) {
                printHelp(options);
            } else {
                String uriFrom = commandLine.getOptionValue("f");
                String uriTo = commandLine.getOptionValue("t");
                String scanMatch = commandLine.getOptionValue("m", "*");
                String SrcCluMod = commandLine.getOptionValue("s", "no");
                String DstCluMod = commandLine.getOptionValue("d", "no");
                int scanLimit = Integer.valueOf(commandLine.getOptionValue("l", "5000"));
                dumpRestore(uriFrom, uriTo, scanMatch, scanLimit, SrcCluMod, DstCluMod);
            }
        } catch (ParseException e) {
            System.err.println("Parsing failed. Reason: " + e.getMessage());
            printHelp(options);
        }
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("blc-redis-dump-restore", "BLC Redis dump restore utility", options, null, true);
    }

    private static void dumpRestore(String uriFrom, String uriTo, String scanMatch, int scanLimit, String SrcCluMod, String DstCluMod) {
        System.out.println("Starting dump and restore with following scanArgs: match=" + scanMatch + ", limit=" + scanLimit);
        System.out.println("From Redis: " + uriFrom);
        System.out.println("To Redis: " + uriTo);
        AtomicLong counter = new AtomicLong();

        RedisKeyCommands<byte[], byte[]> commandsFromSync = null;
        RedisKeyReactiveCommands<byte[], byte[]> commandsFrom = null;
        RedisKeyReactiveCommands<byte[], byte[]> commandsTo = null;

        if ("yes".equals(SrcCluMod)) {
            RedisClusterClient clientFrom = RedisClusterClient.create(uriFrom);
            StatefulRedisClusterConnection<byte[], byte[]> connectFrom = clientFrom.connect(new ByteArrayCodec());
            commandsFromSync = connectFrom.sync();
            commandsFrom = connectFrom.reactive();
        } else {
            RedisClient clientFrom = RedisClient.create(uriFrom);
            StatefulRedisConnection<byte[], byte[]> connectFrom = clientFrom.connect(new ByteArrayCodec());
            commandsFromSync = connectFrom.sync();
            commandsFrom = connectFrom.reactive();
        }

        if ("yes".equals(DstCluMod)) {
            RedisClusterClient clientTo = RedisClusterClient.create(uriTo);
            StatefulRedisClusterConnection<byte[], byte[]> connectTo = clientTo.connect(new ByteArrayCodec());
            commandsTo = connectTo.reactive();
        } else {
            RedisClient clientTo = RedisClient.create(uriTo);
            StatefulRedisConnection<byte[], byte[]> connectTo = clientTo.connect(new ByteArrayCodec());
            commandsTo = connectTo.reactive();
        }

        ScanArgs scanArgs = ScanArgs.Builder.limit(scanLimit).match(scanMatch);
        KeyScanCursor<byte[]> scanCursor = commandsFromSync.scan(scanArgs);
        processKeys(counter, scanCursor.getKeys(), commandsFrom, commandsTo)
                .block();
        while (!scanCursor.isFinished()) {
            scanCursor = commandsFromSync.scan(scanCursor, scanArgs);
            processKeys(counter, scanCursor.getKeys(), commandsFrom, commandsTo)
                    .block();
        }
    }

    private static Mono<byte[]> processKeys(
            AtomicLong counter,
            List<byte[]> keys,
            RedisKeyReactiveCommands<byte[], byte[]> commandsFrom,
            RedisKeyReactiveCommands<byte[], byte[]> commandsTo) {
        return Flux.fromIterable(keys)
                .flatMap(key ->
                        Mono.zip(
                                commandsFrom.dump(key),
                                commandsFrom.pttl(key))
                                .flatMap(result ->
                                        commandsTo.exists(key)
                                                .flatMap(exists -> {
                                                    if (exists == 0) {
                                                        long ttl = result.getT2();
                                                        return commandsTo.restore(key, ttl >= 0 ? ttl : 0, result.getT1());
                                                    } else {
                                                        return Mono.empty();
                                                    }
                                                })))
                .collectList()
                .doAfterSuccessOrError((strings, throwable) -> {
                    System.out.println("Processed new batch, total processed key count: " + counter.addAndGet(keys.size()) + " (dumped-restored keys in this batch: " + strings.size() + ")");
                })
                .ignoreElement()
                .cast(byte[].class);
    }
}
