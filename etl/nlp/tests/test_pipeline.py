def test_run_batch_new_signature_dry_run(pg_conn):
    from etl.nlp.pipeline import BatchStats, run_batch

    stats = run_batch(pg_conn, limit=5, force=False, dry_run=True)
    assert isinstance(stats, BatchStats)
    assert stats.processed == 0
    assert hasattr(stats, "planned")
    assert isinstance(stats.planned, list)


def test_run_batch_order_by_id_desc(pg_conn):
    """SELECT_PENDING devuelve items ordenados por id DESC."""
    from etl.nlp.pipeline import select_pending

    cur = pg_conn.cursor()
    cur.execute("DELETE FROM l0.nacional_subvenciones WHERE id IN (-90001, -90002, -90003)")
    cur.execute(
        """
        INSERT INTO l0.nacional_subvenciones (id, descripcion, url_bases_reguladoras, ingested_at)
        VALUES (-90001, 't1', 'https://e.org/a', '2025-01-01'::timestamptz),
               (-90002, 't2', 'https://e.org/b', '2025-01-01'::timestamptz),
               (-90003, 't3', 'https://e.org/c', '2025-01-01'::timestamptz)
        """
    )
    pg_conn.commit()
    rows = select_pending(pg_conn, limit=10)
    ids = [r[0] for r in rows]
    test_ids = [i for i in ids if i in (-90001, -90002, -90003)]
    assert test_ids == sorted(test_ids, reverse=True)
    cur.execute("DELETE FROM l0.nacional_subvenciones WHERE id IN (-90001, -90002, -90003)")
    pg_conn.commit()
