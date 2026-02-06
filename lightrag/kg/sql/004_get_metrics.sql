-- lightrag_get_metrics: Aggregate metrics across all workspaces server-side.
-- Eliminates N*5+ round-trips by performing all counts in PostgreSQL.
--
-- Parameters:
--   p_workspace: workspace identifier (NULL for all workspaces)
--
-- Returns: JSONB with aggregated metrics for Prometheus export

CREATE OR REPLACE FUNCTION lightrag_get_metrics(
    p_workspace VARCHAR DEFAULT NULL
) RETURNS JSONB AS $$
DECLARE
    v_doc_stats JSONB;
    v_queue_depth INT;
    v_graph_stats JSONB;
    v_workspace_count INT;
BEGIN
    -- Document status metrics (single aggregated query)
    -- Counts ALL documents including duplicates for visibility
    SELECT jsonb_build_object(
        'pending', COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0),
        'processing', COALESCE(SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END), 0),
        'processed', COALESCE(SUM(CASE WHEN status = 'processed' THEN 1 ELSE 0 END), 0),
        'failed', COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0),
        'preprocessed', COALESCE(SUM(CASE WHEN status = 'preprocessed' THEN 1 ELSE 0 END), 0)
    )
    INTO v_doc_stats
    FROM LIGHTRAG_DOC_STATUS
    WHERE p_workspace IS NULL OR workspace = p_workspace;

    -- Queue depth: pending + failed documents EXCLUDING duplicates
    -- Duplicates are not processable (they have no content in full_docs)
    SELECT COALESCE(COUNT(*), 0)
    INTO v_queue_depth
    FROM LIGHTRAG_DOC_STATUS
    WHERE (p_workspace IS NULL OR workspace = p_workspace)
      AND status IN ('pending', 'failed')
      AND (metadata->>'is_duplicate' IS NULL OR metadata->>'is_duplicate' != 'true');

    -- Graph metrics (nodes and edges)
    SELECT jsonb_build_object(
        'nodes', COALESCE((
            SELECT COUNT(*)
            FROM lightrag_graph_nodes
            WHERE p_workspace IS NULL OR workspace = p_workspace
        ), 0),
        'edges', COALESCE((
            SELECT COUNT(*)
            FROM lightrag_graph_edges
            WHERE p_workspace IS NULL OR workspace = p_workspace
        ), 0)
    )
    INTO v_graph_stats;

    -- Count distinct workspaces
    SELECT COUNT(DISTINCT workspace)
    INTO v_workspace_count
    FROM LIGHTRAG_DOC_STATUS
    WHERE p_workspace IS NULL OR workspace = p_workspace;

    RETURN jsonb_build_object(
        'status', 'ok',
        'documents', v_doc_stats,
        'graph', v_graph_stats,
        'workspace_count', v_workspace_count,
        'queue_depth', v_queue_depth
    );
END;
$$ LANGUAGE plpgsql;
