/*
 * Copyright 2018 Federico Valeri.
 * Licensed under the Apache License 2.0 (see LICENSE file).
 */
package it.fvaleri.qstorage.examples;

import it.fvaleri.qstorage.QueryableStorage;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDate;
import java.util.List;
import java.util.Properties;

/**
 * We are using one shared connection because this app is single threaded,
 * but this is not recommended in case of multiple threads querying the database.
 */
public class RunPagamento {
    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(
                "jdbc:h2:mem:test;INIT=runscript from 'classpath:/init.sql'")) {

            long totalRows = 10_000;
            int batchSize = 100;

            Properties queries = new Properties();
            queries.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("pagamento.properties"));

            try (QueryableStorage storage = QueryableStorage.create(conn, queries)) {
                PagamentoDao pagamentoDao = new PagamentoDao(storage, batchSize);

                for (int i = 0; i < totalRows; i++) {
                    pagamentoDao.insert(new Pagamento(i + 1, 1, new BigDecimal(100), LocalDate.now(), 1, 1, "000123456", "AAA"));
                }

                System.out.println(pagamentoDao.findByPk(1));
                System.out.println(pagamentoDao.findByPk(100));
                System.out.println(pagamentoDao.findByPk(1_000));
                System.out.println(pagamentoDao.findByPk(10_000));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class PagamentoDao {
        private QueryableStorage storage;
        private int batchSize;

        public PagamentoDao(QueryableStorage storage, int batchSize) {
            this.storage = storage;
            this.batchSize = batchSize;
        }

        public int insert(Pagamento pagamento) {
            if (pagamento == null) {
                throw new IllegalArgumentException("Invalid pagamento");
            }
            return storage.write(
                    "pagamento.insert",
                    List.of(
                            pagamento.pagCodice(),
                            pagamento.pagIntCodice(),
                            pagamento.pagImporto(),
                            pagamento.pagDataPagamento(),
                            pagamento.pagCpCodice(),
                            pagamento.pagStato(),
                            pagamento.pagCc(),
                            pagamento.pagSisareTipo()
                    ),
                    batchSize
            );
        }

        public Pagamento findByPk(long key) {
            if (key == 0) {
                throw new IllegalArgumentException("Invalid key");
            }
            List<QueryableStorage.Row> rows = storage.read(
                    "pagamento.select.by.pk",
                    List.of(Long.class, Long.class, BigDecimal.class, LocalDate.class,
                            Long.class, Long.class, String.class, String.class),
                    List.of(String.valueOf(key))
            );
            if (rows.size() == 0) {
                return null;
            }
            return new Pagamento(
                    (Long) rows.get(0).columns().get(0),
                    (Long) rows.get(0).columns().get(1),
                    (BigDecimal) rows.get(0).columns().get(2),
                    (LocalDate) rows.get(0).columns().get(3),
                    (Long) rows.get(0).columns().get(4),
                    (Long) rows.get(0).columns().get(5),
                    (String) rows.get(0).columns().get(6),
                    (String) rows.get(0).columns().get(7)
            );
        }

        public int update(Pagamento pagamento) {
            if (pagamento == null) {
                throw new IllegalArgumentException("Invalid pagamento");
            }
            return storage.write(
                    "pagamento.update",
                    List.of(
                            pagamento.pagIntCodice(),
                            pagamento.pagImporto(),
                            pagamento.pagDataPagamento(),
                            pagamento.pagCpCodice(),
                            pagamento.pagStato(),
                            pagamento.pagCc(),
                            pagamento.pagSisareTipo(),
                            pagamento.pagCodice()
                    ),
                    batchSize
            );
        }
    }

    record Pagamento(
            long pagCodice,
            long pagIntCodice,
            BigDecimal pagImporto,
            LocalDate pagDataPagamento,
            long pagCpCodice,
            long pagStato,
            String pagCc,
            String pagSisareTipo) { }
}
