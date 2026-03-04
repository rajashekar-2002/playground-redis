// package com.example.frontend.TraceListener;

// import com.example.frontend.TraceListener.producer.TraceProducer;
// import org.springframework.boot.CommandLineRunner;
// import org.springframework.stereotype.Component;

// @Component
// public class TraceTestRunner implements CommandLineRunner {

// private final TraceProducer traceProducer;
// private final ServiceIdGenerator serviceIdGenerator;

// public TraceTestRunner(TraceProducer traceProducer, ServiceIdGenerator
// serviceIdGenerator) {
// this.traceProducer = traceProducer;
// this.serviceIdGenerator = serviceIdGenerator;
// }

// @Override
// public void run(String... args) throws Exception {
// traceProducer.sendTrace("Test message on startup",
// serviceIdGenerator.getServiceId());
// System.out.println("```````````111111111111111111111Trace message sent on
// startup!");
// }
// }
