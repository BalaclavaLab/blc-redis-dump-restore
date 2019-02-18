package com.balaclavalab.redis;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
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
        options.addOption("f", "uriFrom", true, "Redis from (e.g. redis://redis-interests001.mint.internal/1)");
        options.addOption("t", "uriTo", true, "Redis to (e.g. redis://localhost/15)");
        options.addOption("m", "scanMatch", true, "Scan Match (default: *)");
        options.addOption("l", "scanLimit", true, "Scan Limit (default: 5000)");

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
                int scanLimit = Integer.valueOf(commandLine.getOptionValue("l", "5000"));
                dumpRestore(uriFrom, uriTo, scanMatch, scanLimit);
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

    private static void dumpRestore(String uriFrom, String uriTo, String scanMatch, int scanLimit) {
        System.out.println("Starting dump and restore with following scanArgs: match=" + scanMatch + ", limit=" + scanLimit);
        System.out.println("From Redis: " + uriFrom);
        System.out.println("To Redis: " + uriTo);
        AtomicLong counter = new AtomicLong();

        RedisClient clientFrom = RedisClient.create(uriFrom);
        StatefulRedisConnection<byte[], byte[]> connectFrom = clientFrom.connect(new ByteArrayCodec());
        RedisCommands<byte[], byte[]> commandsFromSync = connectFrom.sync();
        RedisReactiveCommands<byte[], byte[]> commandsFrom = connectFrom.reactive();

        RedisClient clientTo = RedisClient.create(uriTo);
        StatefulRedisConnection<byte[], byte[]> connectTo = clientTo.connect(new ByteArrayCodec());
        RedisReactiveCommands<byte[], byte[]> commandsTo = connectTo.reactive();

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
            RedisReactiveCommands<byte[], byte[]> commandsFrom,
            RedisReactiveCommands<byte[], byte[]> commandsTo) {
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
