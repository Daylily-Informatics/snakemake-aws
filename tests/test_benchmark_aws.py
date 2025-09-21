from snakemake.benchmark import (
    AwsMetricsError,
    AwsSystemMetrics,
    BenchmarkRecord,
    write_benchmark_records,
)


def _build_record():
    record = BenchmarkRecord(
        running_time=60,
        max_rss=1,
        max_vms=1,
        max_uss=1,
        max_pss=1,
        io_in=1,
        io_out=1,
        cpu_usage=120,
        cpu_time=90,
        threads=4,
        input=[],
    )
    record.data_collected = True
    return record


def test_header_includes_aws_columns():
    header = BenchmarkRecord.get_header(include_aws_metrics=True)
    assert header[:10] == [
        "s",
        "h:m:s",
        "max_rss",
        "max_vms",
        "max_uss",
        "max_pss",
        "io_in",
        "io_out",
        "mean_load",
        "cpu_time",
    ]
    assert header[-9:] == [
        "hostname",
        "ip",
        "nproc",
        "cpu_efficiency",
        "instance_type",
        "region_az",
        "spot_cost",
        "snakemake_threads",
        "task_cost",
    ]


def test_write_benchmark_records_with_aws_metrics(tmp_path, monkeypatch):
    record = _build_record()

    def fake_collect():
        return AwsSystemMetrics(
            hostname="test-host",
            ip="1.2.3.4",
            nproc=8,
            instance_type="m5.large",
            region_az="us-east-1a",
            spot_cost="0.1",
        )

    monkeypatch.setattr(
        "snakemake.benchmark.collect_aws_benchmark_metrics", fake_collect
    )

    path = tmp_path / "bench.tsv"
    write_benchmark_records([record], str(path), extended_fmt=False, include_aws_metrics=True)

    lines = path.read_text().strip().splitlines()
    assert "hostname" in lines[0]
    assert "test-host" in lines[1]
    assert "0.100000" in lines[1]
    assert "1.5000" in lines[1]  # cpu_efficiency
    assert "0.000833" in lines[1]  # task_cost


def test_write_benchmark_records_fallback(tmp_path, monkeypatch, caplog):
    record = _build_record()

    def fail_collect():
        raise AwsMetricsError("no metadata")

    monkeypatch.setattr(
        "snakemake.benchmark.collect_aws_benchmark_metrics", fail_collect
    )

    path = tmp_path / "bench.tsv"
    write_benchmark_records([record], str(path), extended_fmt=False, include_aws_metrics=True)

    lines = path.read_text().strip().splitlines()
    assert "hostname" not in lines[0]
    assert any("AWS metrics could not be collected" in message for message in caplog.messages)
