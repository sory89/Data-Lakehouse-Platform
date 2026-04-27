package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"io"
	"net/http"
	"net/url"
	"strings"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var db *pgxpool.Pool


// ── ML Handlers ──────────────────────────────────────────────────────────────

func handleCustomerML(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 6 {
		http.Error(w, "customer_id required", 400)
		return
	}
	customerID := parts[5]

	row := db.QueryRow(r.Context(), `
		SELECT customer_id, order_count_30d, avg_basket_size,
		       churn_probability, clv_score, preferred_category,
		       days_since_last, computed_at
		FROM ml.customer_features
		WHERE customer_id = $1`, customerID)

	var result struct {
		CustomerID        string    `json:"customer_id"`
		OrderCount30d     int       `json:"order_count_30d"`
		AvgBasketSize     float64   `json:"avg_basket_size"`
		ChurnProbability  float64   `json:"churn_probability"`
		ClvScore          float64   `json:"clv_score"`
		PreferredCategory string    `json:"preferred_category"`
		DaysSinceLast     int       `json:"days_since_last"`
		ComputedAt        time.Time `json:"computed_at"`
		RiskLevel         string    `json:"risk_level"`
	}

	if err := row.Scan(&result.CustomerID, &result.OrderCount30d,
		&result.AvgBasketSize, &result.ChurnProbability, &result.ClvScore,
		&result.PreferredCategory, &result.DaysSinceLast, &result.ComputedAt); err != nil {
		http.Error(w, "Customer not found", 404)
		return
	}

	switch {
	case result.ChurnProbability > 0.7:
		result.RiskLevel = "high"
	case result.ChurnProbability > 0.4:
		result.RiskLevel = "medium"
	default:
		result.RiskLevel = "low"
	}

	json.NewEncoder(w).Encode(result)
}

func handleChurnRisk(w http.ResponseWriter, r *http.Request) {
	limit := 20
	rows, err := db.Query(r.Context(), `
		SELECT customer_id, churn_probability, clv_score,
		       days_since_last, preferred_category
		FROM ml.customer_features
		WHERE churn_probability > 0.5
		ORDER BY churn_probability DESC
		LIMIT $1`, limit)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()

	type ChurnRecord struct {
		CustomerID        string  `json:"customer_id"`
		ChurnProbability  float64 `json:"churn_probability"`
		ClvScore          float64 `json:"clv_score"`
		DaysSinceLast     int     `json:"days_since_last"`
		PreferredCategory string  `json:"preferred_category"`
	}

	var results []ChurnRecord
	for rows.Next() {
		var rec ChurnRecord
		rows.Scan(&rec.CustomerID, &rec.ChurnProbability, &rec.ClvScore,
			&rec.DaysSinceLast, &rec.PreferredCategory)
		results = append(results, rec)
	}
	if results == nil {
		results = []ChurnRecord{}
	}
	json.NewEncoder(w).Encode(results)
}


// ── ClickHouse Analytics Handlers ────────────────────────────────────────────

func handleClickHouseStats(w http.ResponseWriter, r *http.Request) {
	chURL := "http://clickhouse:8123/?database=lakehouse&user=lakehouse&password=lakehouse&default_format=JSON&query="
	query := `SELECT count() as total_rows, sum(total_revenue) as total_revenue, max(date) as latest_date FROM lakehouse.daily_sales`

	resp, err := http.Get(chURL + url.QueryEscape(query))
	if err != nil {
		// ClickHouse not available - return empty
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "clickhouse_unavailable",
			"message": "ClickHouse not yet synced",
		})
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, resp.Body)
}

func handleClickHouseTopProducts(w http.ResponseWriter, r *http.Request) {
	chURL := "http://clickhouse:8123/?database=lakehouse&user=lakehouse&password=lakehouse&default_format=JSON&query="
	query := `SELECT product_name, category, sum(total_revenue) as revenue, sum(units_sold) as units FROM lakehouse.product_performance GROUP BY product_name, category ORDER BY revenue DESC LIMIT 10`

	resp, err := http.Get(chURL + url.QueryEscape(query))
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]interface{}{})
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, resp.Body)
}

func handleClickHouseChurnStats(w http.ResponseWriter, r *http.Request) {
	chURL := "http://clickhouse:8123/?database=lakehouse&user=lakehouse&password=lakehouse&default_format=JSON&query="
	query := `SELECT countIf(churn_probability > 0.7) as high_risk, countIf(churn_probability BETWEEN 0.4 AND 0.7) as medium_risk, countIf(churn_probability < 0.4) as low_risk, avg(churn_probability) as avg_churn, avg(clv_score) as avg_clv FROM lakehouse.customer_segments`

	resp, err := http.Get(chURL + url.QueryEscape(query))
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"status": "unavailable"})
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, resp.Body)
}

func handleMLSummary(w http.ResponseWriter, r *http.Request) {
	var result struct {
		TotalCustomers    int     `json:"total_customers"`
		HighRisk          int     `json:"high_risk_count"`
		MediumRisk        int     `json:"medium_risk_count"`
		LowRisk           int     `json:"low_risk_count"`
		AvgChurnProb      float64 `json:"avg_churn_probability"`
		AvgClvScore       float64 `json:"avg_clv_score"`
	}

	db.QueryRow(r.Context(), `
		SELECT
			COUNT(*),
			SUM(CASE WHEN churn_probability > 0.7 THEN 1 ELSE 0 END),
			SUM(CASE WHEN churn_probability BETWEEN 0.4 AND 0.7 THEN 1 ELSE 0 END),
			SUM(CASE WHEN churn_probability < 0.4 THEN 1 ELSE 0 END),
			ROUND(AVG(churn_probability)::numeric, 4),
			ROUND(AVG(clv_score)::numeric, 2)
		FROM ml.customer_features
	`).Scan(&result.TotalCustomers, &result.HighRisk, &result.MediumRisk,
		&result.LowRisk, &result.AvgChurnProb, &result.AvgClvScore)

	json.NewEncoder(w).Encode(result)
}

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://lakehouse:lakehouse@postgres:5432/lakehouse"
	}

	var err error
	for i := 0; i < 10; i++ {
		db, err = pgxpool.New(context.Background(), dsn)
		if err == nil {
			if pingErr := db.Ping(context.Background()); pingErr == nil {
				break
			}
		}
		log.Printf("DB not ready, retrying in 3s... (%d/10)", i+1)
		time.Sleep(3 * time.Second)
	}
	if err != nil {
		log.Fatalf("Cannot connect to DB: %v", err)
	}
	defer db.Close()

	log.Println("Lakehouse API started — listening on :8080")

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health",                    handleHealth)
	mux.HandleFunc("GET /api/v1/daily-sales",         handleDailySales)
	mux.HandleFunc("GET /api/v1/revenue-trends",      handleRevenueTrends)
	mux.HandleFunc("GET /api/v1/top-products",         handleTopProducts)
	mux.HandleFunc("GET /api/v1/customer-segments",   handleCustomerSegments)
	mux.HandleFunc("GET /api/v1/customer/{id}/ltv",   handleCustomerLTV)
	mux.HandleFunc("GET /api/v1/pipeline-runs",       handlePipelineRuns)
	mux.HandleFunc("GET /api/v1/catalog",             handleCatalog)
	mux.HandleFunc("GET /api/v1/dq-results",          handleDQResults)
	mux.HandleFunc("GET /api/v1/summary",             handleSummary)

	// ClickHouse analytics endpoints
	mux.HandleFunc("GET /api/v1/clickhouse/stats",        handleClickHouseStats)
	mux.HandleFunc("GET /api/v1/clickhouse/top-products", handleClickHouseTopProducts)
	mux.HandleFunc("GET /api/v1/clickhouse/churn-stats",  handleClickHouseChurnStats)

	// ML endpoints
	mux.HandleFunc("GET /api/v1/ml/features/summary",  handleMLSummary)
	mux.HandleFunc("GET /api/v1/ml/churn-risk",        handleChurnRisk)
	mux.HandleFunc("GET /api/v1/ml/customer/",         handleCustomerML)

	if err := http.ListenAndServe(":8080", cors(mux)); err != nil {
		log.Fatal(err)
	}
}

// ── Middleware ────────────────────────────────────────────────────────────────

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

func respond(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func respondError(w http.ResponseWriter, code int, msg string) {
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// ── Handlers ──────────────────────────────────────────────────────────────────

func handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := db.Ping(r.Context()); err != nil {
		respondError(w, 503, "db unreachable")
		return
	}
	respond(w, map[string]string{"status": "ok", "service": "lakehouse-api", "version": "1.0.0"})
}

func handleSummary(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var (
		totalRevenue    float64
		totalOrders     int64
		avgOrder        float64
		uniqueCustomers int64
	)

	db.QueryRow(ctx, `
		SELECT
		  COALESCE(SUM(total_revenue),0),
		  COALESCE(SUM(total_orders),0),
		  COALESCE(AVG(avg_order),0),
		  COALESCE(MAX(unique_customers),0)
		FROM gold.daily_sales
		WHERE date >= CURRENT_DATE - INTERVAL '30 days'
	`).Scan(&totalRevenue, &totalOrders, &avgOrder, &uniqueCustomers)

	var vipCount, atRiskCount int64
	db.QueryRow(ctx, `SELECT COUNT(*) FROM gold.customer_segments WHERE segment='vip'`).Scan(&vipCount)
	db.QueryRow(ctx, `SELECT COUNT(*) FROM gold.customer_segments WHERE segment='at_risk'`).Scan(&atRiskCount)

	respond(w, map[string]any{
		"period":           "last_30_days",
		"total_revenue":    totalRevenue,
		"total_orders":     totalOrders,
		"avg_order_value":  avgOrder,
		"unique_customers": uniqueCustomers,
		"vip_customers":    vipCount,
		"at_risk_customers": atRiskCount,
		"generated_at":    time.Now().UTC(),
	})
}

func handleDailySales(w http.ResponseWriter, r *http.Request) {
	days := 30
	if d := r.URL.Query().Get("days"); d != "" {
		if v, err := strconv.Atoi(d); err == nil {
			days = v
		}
	}
	rows, err := db.Query(r.Context(), `
		SELECT date, total_orders, total_revenue, avg_order, unique_customers, top_category
		FROM gold.daily_sales
		WHERE date >= CURRENT_DATE - ($1 || ' days')::interval
		ORDER BY date DESC
	`, fmt.Sprintf("%d", days))
	if err != nil {
		respondError(w, 500, err.Error()); return
	}
	defer rows.Close()

	result := []map[string]any{}
	for rows.Next() {
		var date time.Time
		var orders, customers int
		var revenue, avg float64
		var category *string
		rows.Scan(&date, &orders, &revenue, &avg, &customers, &category)
		result = append(result, map[string]any{
			"date": date.Format("2006-01-02"), "total_orders": orders,
			"total_revenue": revenue, "avg_order": avg,
			"unique_customers": customers, "top_category": category,
		})
	}
	respond(w, result)
}

func handleRevenueTrends(w http.ResponseWriter, r *http.Request) {
	// Try dbt view first, fallback to raw gold table
	query := `
		SELECT date, total_revenue,
		       COALESCE(revenue_7d_avg, total_revenue) AS revenue_7d_avg,
		       COALESCE(revenue_7d_sum, total_revenue) AS revenue_7d_sum,
		       mom_growth_pct
		FROM gold.revenue_trends
		ORDER BY date DESC LIMIT 90
	`
	rows, err := db.Query(r.Context(), query)
	if err != nil {
		// Fallback: compute from daily_sales
		rows, err = db.Query(r.Context(), `
			SELECT date, total_revenue,
			       AVG(total_revenue) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS revenue_7d_avg,
			       SUM(total_revenue) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS revenue_7d_sum,
			       NULL::float AS mom_growth_pct
			FROM gold.daily_sales
			ORDER BY date DESC LIMIT 90
		`)
		if err != nil {
			respondError(w, 500, err.Error()); return
		}
	}
	defer rows.Close()

	result := []map[string]any{}
	for rows.Next() {
		var date time.Time
		var rev, avg7, sum7 float64
		var mom *float64
		rows.Scan(&date, &rev, &avg7, &sum7, &mom)
		result = append(result, map[string]any{
			"date": date.Format("2006-01-02"), "total_revenue": rev,
			"revenue_7d_avg": avg7, "revenue_7d_sum": sum7, "mom_growth_pct": mom,
		})
	}
	respond(w, result)
}

func handleTopProducts(w http.ResponseWriter, r *http.Request) {
	limit := 10
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil {
			limit = v
		}
	}
	period := r.URL.Query().Get("period")

	var (
		rows pgx.Rows
		err  error
	)

	if period != "" {
		rows, err = db.Query(r.Context(), `
			SELECT product_id, product_name, category, period,
			       units_sold, revenue, avg_price, return_rate_pct, revenue_rank
			FROM gold.top_products
			WHERE period=$1
			ORDER BY revenue_rank ASC LIMIT $2
		`, period, limit)
	} else {
		rows, err = db.Query(r.Context(), `
			SELECT product_id, product_name, category, period,
			       units_sold, revenue, avg_price, return_rate_pct, revenue_rank
			FROM gold.top_products
			WHERE period = (SELECT MAX(period) FROM gold.top_products)
			ORDER BY revenue_rank ASC LIMIT $1
		`, limit)
		if err != nil {
			// Fallback to product_performance
			rows, err = db.Query(r.Context(), `
				SELECT product_id, COALESCE(product_name,'Unknown'), COALESCE(category,'Unknown'),
				       period, units_sold, revenue, avg_price,
				       ROUND(return_rate*100,2), 
				       RANK() OVER (ORDER BY revenue DESC)
				FROM gold.product_performance
				WHERE period = (SELECT MAX(period) FROM gold.product_performance)
				ORDER BY revenue DESC LIMIT $1
			`, limit)
			if err != nil {
				respondError(w, 500, err.Error()); return
			}
		}
	}
	if err != nil {
		respondError(w, 500, err.Error()); return
	}
	defer rows.Close()

	result := []map[string]any{}
	for rows.Next() {
		var pid, units, rank int
		var name, cat, per string
		var rev, avgP, retRate float64
		rows.Scan(&pid, &name, &cat, &per, &units, &rev, &avgP, &retRate, &rank)
		result = append(result, map[string]any{
			"product_id": pid, "product_name": name, "category": cat,
			"period": per, "units_sold": units, "revenue": rev,
			"avg_price": avgP, "return_rate_pct": retRate, "rank": rank,
		})
	}
	respond(w, result)
}

func handleCustomerSegments(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(r.Context(), `
		SELECT segment, COUNT(*) AS count,
		       ROUND(AVG(total_spent),2) AS avg_spent,
		       ROUND(AVG(order_count),1) AS avg_orders,
		       ROUND(AVG(clv_score),4)   AS avg_clv
		FROM gold.customer_segments
		GROUP BY segment ORDER BY avg_spent DESC
	`)
	if err != nil {
		respondError(w, 500, err.Error()); return
	}
	defer rows.Close()

	result := []map[string]any{}
	for rows.Next() {
		var seg string
		var count int64
		var avgSpent, avgOrders, avgClv float64
		rows.Scan(&seg, &count, &avgSpent, &avgOrders, &avgClv)
		result = append(result, map[string]any{
			"segment": seg, "count": count, "avg_spent": avgSpent,
			"avg_orders": avgOrders, "avg_clv": avgClv,
		})
	}
	respond(w, result)
}

func handleCustomerLTV(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	cid, err := strconv.Atoi(id)
	if err != nil {
		respondError(w, 400, "invalid customer_id"); return
	}

	var seg, rfmSeg string
	var rfmScore, totalSpent, avgOrder, clv float64
	var orderCount int
	var lastOrder *time.Time

	err = db.QueryRow(r.Context(), `
		SELECT segment, rfm_segment, rfm_score, total_spent,
		       avg_order_value, order_count, last_order_at, clv_score
		FROM gold.customer_ltv WHERE customer_id=$1
	`, cid).Scan(&seg, &rfmSeg, &rfmScore, &totalSpent, &avgOrder, &orderCount, &lastOrder, &clv)

	if err != nil {
		respondError(w, 404, "customer not found"); return
	}

	respond(w, map[string]any{
		"customer_id": cid, "segment": seg, "rfm_segment": rfmSeg,
		"rfm_score": rfmScore, "total_spent": totalSpent,
		"avg_order_value": avgOrder, "order_count": orderCount,
		"last_order_at": lastOrder, "clv_score": clv,
	})
}

func handlePipelineRuns(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(r.Context(), `
		SELECT id, pipeline_name, layer, status, rows_read, rows_written,
		       duration_ms, started_at, finished_at
		FROM pipeline_runs
		ORDER BY started_at DESC LIMIT 50
	`)
	if err != nil {
		respondError(w, 500, err.Error()); return
	}
	defer rows.Close()

	result := []map[string]any{}
	for rows.Next() {
		var id, rowsR, rowsW int64
		var ms *int
		var name, layer, status string
		var started time.Time
		var finished *time.Time
		rows.Scan(&id, &name, &layer, &status, &rowsR, &rowsW, &ms, &started, &finished)
		result = append(result, map[string]any{
			"id": id, "pipeline_name": name, "layer": layer, "status": status,
			"rows_read": rowsR, "rows_written": rowsW, "duration_ms": ms,
			"started_at": started, "finished_at": finished,
		})
	}
	respond(w, result)
}

func handleCatalog(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(r.Context(), `
		SELECT table_name, layer, location, format, row_count, size_bytes, last_updated
		FROM table_catalog ORDER BY layer, table_name
	`)
	if err != nil {
		respondError(w, 500, err.Error()); return
	}
	defer rows.Close()

	result := []map[string]any{}
	for rows.Next() {
		var name, layer, loc, fmt string
		var rows_, size int64
		var updated time.Time
		rows.Scan(&name, &layer, &loc, &fmt, &rows_, &size, &updated)
		result = append(result, map[string]any{
			"table_name": name, "layer": layer, "location": loc,
			"format": fmt, "row_count": rows_, "size_bytes": size, "last_updated": updated,
		})
	}
	respond(w, result)
}

func handleDQResults(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(r.Context(), `
		SELECT table_name, layer, check_name, status, rows_tested, rows_failed, run_at
		FROM dq_results ORDER BY run_at DESC LIMIT 100
	`)
	if err != nil {
		respondError(w, 500, err.Error()); return
	}
	defer rows.Close()

	result := []map[string]any{}
	for rows.Next() {
		var tbl, layer, check, status string
		var tested, failed int64
		var runAt time.Time
		rows.Scan(&tbl, &layer, &check, &status, &tested, &failed, &runAt)
		result = append(result, map[string]any{
			"table_name": tbl, "layer": layer, "check_name": check,
			"status": status, "rows_tested": tested, "rows_failed": failed, "run_at": runAt,
		})
	}
	respond(w, result)
}
