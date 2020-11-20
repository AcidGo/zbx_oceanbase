# ---------- CONFG ----------
cls_config = {
    "f5-dgbob": {
        "host": "127.0.0.1",
        "port": 2883,
        "cluster": "testcl",
        "user": "testcl:sys:query",
        "password": "passwd",
    },
}
backup_restore_config = {
    "bakrst_inst_1": {
        "host": "127.0.0.1",
        "port": 2883,
        "user": "obcluster:ocp_meta:query",
        "password": "passwd",
        # choose backup-restore platform meta database
        "database": "backup2230",
    }
}
# ========== CONFG ==========
