-- ============================================
-- FK 정합성 체크 (Foreign Key Integrity Check)
-- ============================================
-- 설명: 외래 키 참조 무결성을 검증합니다.
-- 사용법: 변수들을 실제 값으로 대체하세요.
-- ============================================

-- 1. 기본 FK 정합성 체크
-- 자식 테이블의 FK가 부모 테이블에 존재하는지 확인합니다.
SELECT 
    '${CHILD_TABLE}' AS child_table,
    '${FK_COLUMN}' AS fk_column,
    '${PARENT_TABLE}' AS parent_table,
    '${PK_COLUMN}' AS pk_column,
    COUNT(*) AS orphan_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result
FROM ${CHILD_TABLE} c
WHERE c.${FK_COLUMN} IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 
    FROM ${PARENT_TABLE} p 
    WHERE p.${PK_COLUMN} = c.${FK_COLUMN}
  );


-- 2. 고아 레코드 상세 조회
-- 참조 무결성을 위반한 레코드를 상세히 조회합니다.
SELECT 
    c.*,
    '참조 대상 없음' AS violation_reason
FROM ${CHILD_TABLE} c
WHERE c.${FK_COLUMN} IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 
    FROM ${PARENT_TABLE} p 
    WHERE p.${PK_COLUMN} = c.${FK_COLUMN}
  )
ORDER BY c.${FK_COLUMN}
LIMIT 100;


-- 3. 복합 FK 정합성 체크
-- 여러 컬럼으로 구성된 복합 FK의 정합성을 확인합니다.
SELECT 
    '${CHILD_TABLE}' AS child_table,
    COUNT(*) AS orphan_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result
FROM ${CHILD_TABLE} c
WHERE (c.${FK_COLUMN_1} IS NOT NULL AND c.${FK_COLUMN_2} IS NOT NULL)
  AND NOT EXISTS (
    SELECT 1 
    FROM ${PARENT_TABLE} p 
    WHERE p.${PK_COLUMN_1} = c.${FK_COLUMN_1}
      AND p.${PK_COLUMN_2} = c.${FK_COLUMN_2}
  );


-- 4. 양방향 참조 체크
-- 두 테이블 간 양방향 참조 무결성을 확인합니다.
WITH orphans_in_child AS (
    SELECT COUNT(*) AS cnt
    FROM ${CHILD_TABLE} c
    WHERE c.${FK_COLUMN} IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM ${PARENT_TABLE} p WHERE p.${PK_COLUMN} = c.${FK_COLUMN}
      )
),
unused_in_parent AS (
    SELECT COUNT(*) AS cnt
    FROM ${PARENT_TABLE} p
    WHERE NOT EXISTS (
        SELECT 1 FROM ${CHILD_TABLE} c WHERE c.${FK_COLUMN} = p.${PK_COLUMN}
    )
)
SELECT 
    '${CHILD_TABLE} -> ${PARENT_TABLE}' AS relationship,
    (SELECT cnt FROM orphans_in_child) AS orphan_records,
    (SELECT cnt FROM unused_in_parent) AS unused_parent_records,
    CASE 
        WHEN (SELECT cnt FROM orphans_in_child) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS integrity_check_result;


-- 5. 순환 참조 체크
-- 자기 참조 테이블에서 순환 참조를 탐지합니다.
WITH RECURSIVE hierarchy AS (
    -- 루트 노드
    SELECT 
        ${ID_COLUMN},
        ${PARENT_ID_COLUMN},
        1 AS level,
        ARRAY[${ID_COLUMN}] AS path
    FROM ${TABLE_NAME}
    WHERE ${PARENT_ID_COLUMN} IS NULL
    
    UNION ALL
    
    -- 자식 노드
    SELECT 
        t.${ID_COLUMN},
        t.${PARENT_ID_COLUMN},
        h.level + 1,
        h.path || t.${ID_COLUMN}
    FROM ${TABLE_NAME} t
    INNER JOIN hierarchy h ON t.${PARENT_ID_COLUMN} = h.${ID_COLUMN}
    WHERE NOT t.${ID_COLUMN} = ANY(h.path)  -- 순환 방지
      AND h.level < 100  -- 최대 깊이 제한
)
SELECT 
    '${TABLE_NAME}' AS table_name,
    COUNT(*) AS records_in_hierarchy,
    (SELECT COUNT(*) FROM ${TABLE_NAME}) AS total_records,
    CASE 
        WHEN COUNT(*) = (SELECT COUNT(*) FROM ${TABLE_NAME}) THEN 'PASS'
        ELSE 'WARNING - 일부 레코드가 계층에서 누락됨 (순환 참조 가능성)'
    END AS check_result
FROM hierarchy;


-- 6. 삭제된 참조 체크 (Soft Delete)
-- Soft Delete된 부모를 참조하는 자식 레코드를 찾습니다.
SELECT 
    '${CHILD_TABLE}' AS child_table,
    COUNT(*) AS referencing_deleted_parent_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END AS check_result
FROM ${CHILD_TABLE} c
INNER JOIN ${PARENT_TABLE} p ON p.${PK_COLUMN} = c.${FK_COLUMN}
WHERE p.${DELETED_FLAG_COLUMN} = TRUE
  AND (c.${DELETED_FLAG_COLUMN} IS NULL OR c.${DELETED_FLAG_COLUMN} = FALSE);


-- 7. FK 값 분포 분석
-- FK 컬럼의 값 분포를 분석하여 데이터 품질을 파악합니다.
SELECT 
    '${CHILD_TABLE}' AS table_name,
    '${FK_COLUMN}' AS fk_column,
    COUNT(*) AS total_rows,
    COUNT(${FK_COLUMN}) AS non_null_fk_count,
    COUNT(DISTINCT ${FK_COLUMN}) AS unique_fk_values,
    (SELECT COUNT(*) FROM ${PARENT_TABLE}) AS parent_table_rows,
    ROUND(
        COUNT(DISTINCT ${FK_COLUMN})::NUMERIC / 
        NULLIF((SELECT COUNT(*) FROM ${PARENT_TABLE}), 0) * 100, 
        2
    ) AS parent_coverage_percentage
FROM ${CHILD_TABLE};


-- 8. 다중 테이블 FK 체크
-- 여러 자식 테이블의 FK 정합성을 한번에 확인합니다.
SELECT 
    'orders' AS child_table,
    'customer_id' AS fk_column,
    COUNT(*) AS orphan_count
FROM orders o
WHERE o.customer_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id)

UNION ALL

SELECT 
    'order_items' AS child_table,
    'product_id' AS fk_column,
    COUNT(*) AS orphan_count
FROM order_items oi
WHERE oi.product_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM products p WHERE p.id = oi.product_id)

UNION ALL

SELECT 
    'order_items' AS child_table,
    'order_id' AS fk_column,
    COUNT(*) AS orphan_count
FROM order_items oi
WHERE oi.order_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.id = oi.order_id);
