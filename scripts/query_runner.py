class QueryRunner:
    def __init__(self, db_manager, exporter):
        self.db = db_manager
        self.exporter = exporter

    def run_all(self, queries, output_path):
        results = {}

        for QueryClass in queries:
            query = QueryClass(self.db)
            name = query.get_query_name()
            data = query.execute()
            results[name] = data

        self.exporter.export(results, output_path)