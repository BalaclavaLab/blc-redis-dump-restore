package com.balaclavalab.redis;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisKeyReactiveCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
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
        options.addOption("fc", "fromCluster", false, "Use cluster connection for Redis from");
        options.addOption("t", "uriTo", true, "Redis to (e.g. redis-cluster://localhost/0)");
        options.addOption("tc", "toCluster", false, "Use cluster connection for Redis to");
        options.addOption("m", "scanMatch", true, "Scan Match (default: *)");
        options.addOption("l", "scanLimit", true, "Scan Limit (default: 5000)");

        CommandLineParser commandLineParser = new DefaultParser();
        try {
            CommandLine commandLine = commandLineParser.parse(options, args);
            boolean hasOptions = commandLine.getOptions().length > 0;
            if (commandLine.hasOption("help") || !hasOptions) {
                printHelp(options);
            } else {
                RedisURI uriFrom = RedisURI.create(commandLine.getOptionValue("f"));
                RedisURI uriTo = RedisURI.create(commandLine.getOptionValue("t"));
                boolean fromCluster = commandLine.hasOption("fc");
                boolean toCluster = commandLine.hasOption("tc");
                String scanMatch = commandLine.getOptionValue("m", "*");
                int scanLimit = Integer.parseInt(commandLine.getOptionValue("l", "5000"));
                dumpRestore(uriFrom, fromCluster, uriTo, toCluster, scanMatch, scanLimit);
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

    private static void dumpRestore(
            RedisURI uriFrom,
            boolean fromCluster,
            RedisURI uriTo,
            boolean toCluster,
            String scanMatch,
            int scanLimit) {
        System.out.println("Starting dump and restore with following scanArgs: match=" + scanMatch + ", limit=" + scanLimit);
        System.out.println("From Redis: " + uriFrom + ", cluster enabled: " + fromCluster);
        System.out.println("To Redis: " + uriTo + ", cluster enabled: " + toCluster);
        AtomicLong counter = new AtomicLong();

        RedisKeyCommands<byte[], byte[]> commandsFromSync;
        RedisKeyReactiveCommands<byte[], byte[]> commandsFrom;
        if (fromCluster) {
            RedisClusterClient clusterClientFrom = RedisClusterClient.create(uriFrom);
            StatefulRedisClusterConnection<byte[], byte[]> connectFrom = clusterClientFrom.connect(new ByteArrayCodec());
            commandsFromSync = connectFrom.sync();
            commandsFrom = connectFrom.reactive();
        } else {
            RedisClient clientFrom = RedisClient.create(uriFrom);
            StatefulRedisConnection<byte[], byte[]> connectFrom = clientFrom.connect(new ByteArrayCodec());
            commandsFromSync = connectFrom.sync();
            commandsFrom = connectFrom.reactive();
        }

        RedisKeyReactiveCommands<byte[], byte[]> commandsTo;
        if (toCluster) {
            RedisClusterClient clusterClientTo = RedisClusterClient.create(uriTo);
            StatefulRedisClusterConnection<byte[], byte[]> connectTo = clusterClientTo.connect(new ByteArrayCodec());
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
