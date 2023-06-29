package org.example;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.RecursiveAction;

class ParallelProcessor extends RecursiveAction {
    private static final int THRESHOLD = 1000;
    private List<String> strings;

    ParallelProcessor(List<String> strings) {
        this.strings = strings;
    }

    @Override
    protected void compute() {
        if (strings.size() <= THRESHOLD) {
            try {
                process(strings);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else {
            int mid = strings.size() / 2;
            List<String> leftSublist = strings.subList(0, mid);
            List<String> rightSublist = strings.subList(mid, strings.size());

            invokeAll(new ParallelProcessor(leftSublist), new ParallelProcessor(rightSublist));
        }
    }

    private void process(List<String> sublist) throws SQLException {
        // Lógica de processamento para cada sublista
        for (String person : sublist) {
            DbConnection.insertPerson(person);
        }
    }
}