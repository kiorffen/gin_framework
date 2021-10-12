package handler

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"strconv"
	"strings"
	"time"

	"gin_framework/common"
	"gin_framework/global"

	"github.com/gin-gonic/gin"

	elastic "gopkg.in/olivere/elastic.v6"
)

type CCTopBasic struct {
	Flag       string `json:"flag"`
	Sort       string `json:"sort"`
	Desc       string `json:"desc"`
	Size       string `json:"size"`
	TopID      string `json:"topid"`
	WeightInfo string `json:"weight"`
	TimeInfo   string `json:"time"`
}

type CCTop struct {
	Basic CCTopBasic        `json:"basic"`
	Need  map[string]string `json:"need"`
}

type CCReqMustWithoutShould struct {
	Need  map[string]string `json:"need"`
	Range map[string]string `json:"range"`
	Match map[string]string `json:"match"`
}

type CCReqMust struct {
	Need   map[string]string        `json:"need"`
	Range  map[string]string        `json:"range"`
	Match  map[string]string        `json:"match"`
	Should []CCReqMustWithoutShould `json:"should"`
}

type CCBasic struct {
	IBiz      string `json:"ibiz"`
	Source    string `json:"source"`
	Timestamp string `json:"t"`
	Sign      string `json:"sign"`
	Sort      string `json:"sort"`
	Desc      string `json:"desc"`
	Page      string `json:"page"`
	Pagesize  string `json:"pagesize"`
	Nc        string `json:"nc"`
	Ns        string `json:"ns"`
}

type CCReqParam struct {
	Basic   CCBasic   `json:"basic"`
	Top     CCTop     `json:"top"`
	Must    CCReqMust `json:"must"`
	MustNot CCReqMust `json:"must_not"`
}

type CCParamData struct {
	Req CCReqParam `json:"req"`
	Res []string   `json:"res"`
}

type SearchHandler struct {
	RootHandler

	Param CCParamData

	IBiz int

	esIndex string
	esType  string

	cacheKey string
}

func (h *SearchHandler) Process(ctx *gin.Context) {
	var err error
	res := make(map[string]interface{})
	items := make([]map[string]interface{}, 0)

	ctx.BindJSON(&h.Param)

	h.IBiz, err = strconv.Atoi(h.Param.Req.Basic.IBiz)
	if err != nil || h.Param.Req.Basic.IBiz == "" {
		h.OutJson(ctx, -1, "invalid ibiz", res)
	}
	if h.Param.Req.Basic.Source == "" {
		h.OutJson(ctx, -1, "invalid source", res)
	}
	if common.CheckSign(h.Param.Req.Basic.Sign,
		h.Param.Req.Basic.Source,
		h.Param.Req.Basic.Timestamp,
		h.IBiz) == false {
		h.OutJson(ctx, -1, "check sign error", res)
	}

	if global.G_env == "release" {
		h.esIndex = "index_online"
		h.esType = "type_online"
	} else {
		h.esIndex = "index_test"
		h.esType = "type_test"
	}

	// search cache
	nc := "yes"
	if h.Param.Req.Basic.Nc == "no" {
		nc = "no"
	}
	ns := "no"
	if h.Param.Req.Basic.Ns == "yes" {
		ns = "yes"
	}
	if nc == "yes" {
		bdata, err := ioutil.ReadAll(ctx.Request.Body)
		if err != nil {
			h.OutJson(ctx, -1, err.Error(), res)
		}
		h.cacheKey = fmt.Sprintf("%X", md5.Sum(bdata))
		cacheData, err := global.G_cache["content_info"].Get(h.cacheKey)
		if err == nil && cacheData != "" {
			var data map[string]interface{}
			err = json.Unmarshal([]byte(cacheData), &data)
			if err == nil {
				h.OutJson(ctx, 0, "OK", data)
			}
		}
	}

	// get top content
	var topDocIDs []interface{}
	if h.Param.Req.Top.Basic.Flag == "yes" {
		topItems, err := h.getTopItems(ctx, nc)
		if err != nil {
			h.OutJson(ctx, -1, "get topItems failed. err: "+err.Error(), res)
		}
		for _, item := range topItems {
			topDocIDs = append(topDocIDs, item["com_docid"])
			items = append(items, item)
		}
	}

	q := elastic.NewBoolQuery()
	// must need
	h.parseMustNeed(ctx, q, h.Param.Req.Must.Need)
	// must range
	h.parseMustRange(ctx, q, h.Param.Req.Must.Range)
	// must match
	h.parseMustMatch(ctx, q, h.Param.Req.Must.Match)
	// must should
	h.parseMustShould(ctx, q, h.Param.Req.Must.Should)

	// must not need
	h.parseMustNotNeed(ctx, q, h.Param.Req.MustNot.Need)
	// must not match
	h.parseMustNotMatch(ctx, q, h.Param.Req.MustNot.Match)
	// must not range
	h.parseMustNotRange(ctx, q, h.Param.Req.MustNot.Range)
	// must not should
	h.parseMustNotShould(ctx, q, h.Param.Req.MustNot.Should)

	// topitems
	if len(topDocIDs) > 0 {
		q = q.MustNot(elastic.NewTermsQuery("com_docid", topDocIDs...))
	}

	query := global.G_es["yxs"].Client.Search().Index(h.esIndex).Type(h.esType).Preference("_primary_first").Timeout("1s").Query(q)
	if h.Param.Req.Basic.Page != "" {
		page, _ := strconv.Atoi(h.Param.Req.Basic.Page)
		pagesize, _ := strconv.Atoi(h.Param.Req.Basic.Pagesize)
		if page == 0 {
			page = 1
		}
		if pagesize == 0 {
			pagesize = 10
		}
		start := (page - 1) * pagesize
		query = query.From(start)
	}
	if h.Param.Req.Basic.Pagesize != "" {
		limit, _ := strconv.Atoi(h.Param.Req.Basic.Pagesize)
		if limit == 0 {
			limit = 10
		}
		query = query.Size(limit)
	}
	if h.Param.Req.Basic.Sort != "" && h.Param.Req.Basic.Desc != "" {
		sorts := strings.Split(h.Param.Req.Basic.Sort, ",")
		descs := strings.Split(h.Param.Req.Basic.Desc, ",")
		if len(sorts) != len(descs) {
			h.OutJson(ctx, -1, "invalid sort and desc", res)
		}
		for idx, d := range descs {
			desc := true
			if d == "yes" {
				desc = false
			}
			sort := sorts[idx]
			query = query.Sort(sort, desc)
		}
	}

	resEs, errEs := query.Do(context.TODO())
	if errEs != nil {
		searchlog := ""
		src, err := q.Source()
		if err == nil {
			bdata, err := json.Marshal(src)
			if err == nil {
				searchlog = string(bdata)
			}
		}
		if global.G_debug {
			fmt.Println(searchlog)
		}
		h.OutJson(ctx, -1, searchlog, res)
	}

	if global.G_debug {
		src, _ := q.Source()
		bdata, _ := json.Marshal(src)
		fmt.Println(string(bdata))
	}

	total := resEs.Hits.TotalHits

	// parseDoc
	for _, hit := range resEs.Hits.Hits {
		item, err := global.G_es["yxs"].ParseDoc(hit.Source, h.Param.Res)
		if err != nil {
			h.OutJson(ctx, -1, "parse doc failed. err: "+err.Error(), res)
			continue
		}
		item["top"] = 0
		if ns == "yes" {
			item["score"] = *hit.Score
		}
		items = append(items, item)
	}

	// create result
	res["items"] = items
	res["total"] = total
	if h.Param.Req.Basic.Page != "" {
		page, _ := strconv.Atoi(h.Param.Req.Basic.Page)
		pagesize, _ := strconv.Atoi(h.Param.Req.Basic.Pagesize)
		if page == 0 {
			page = 1
		}
		if pagesize == 0 {
			pagesize = 10
		}
		start := (page - 1) * pagesize
		res["page"] = page
		res["totalpage"] = math.Ceil(float64(total) / float64(pagesize))
		res["pagesize"] = math.Max(math.Min(float64(int(total)-start), float64(pagesize)), 0)
	}

	if nc == "yes" {
		bdata, err := json.Marshal(res)
		if err == nil {
			global.G_cache["content_info"].Set(h.cacheKey, string(bdata))
		}
	}

	h.OutJson(ctx, 0, "OK", res)
}

func (h *SearchHandler) parseMustNeed(ctx *gin.Context, q *elastic.BoolQuery, need map[string]string) {
	for field, value := range need {
		q = q.Must(elastic.NewTermsQuery(field, common.ParseStringToInterface(value)...))
	}
}

func (h *SearchHandler) parseMustRange(ctx *gin.Context, q *elastic.BoolQuery, rg map[string]string) {
	for field, value := range rg {
		parts := strings.Split(value, "|")
		if len(parts) != 3 {
			h.OutJson(ctx, -1, "invalid range: "+value, "")
		}
		switch parts[2] {
		case "T":
			stime := ""
			etime := ""
			if parts[0] != "" {
				tt, err := strconv.ParseInt(parts[0], 10, 64)
				if err != nil {
					h.OutJson(ctx, -1, "invalid stime "+parts[0], "")
				}
				stime = time.Unix(tt, 0).Format("2006-01-02 15:04:05")
			}
			if parts[1] != "" {
				tt, err := strconv.ParseInt(parts[1], 10, 64)
				if err != nil {
					h.OutJson(ctx, -1, "invalid etime "+parts[1], "")
				}
				etime = time.Unix(tt, 0).Format("2006-01-02 15:04:05")
			}
			q = q.Filter(elastic.NewRangeQuery(field).Format("yyyy-MM-dd HH:mm:ss").Gte(stime))
			q = q.Filter(elastic.NewRangeQuery(field).Format("yyyy-MM-dd HH:mm:ss").Lte(etime))
		case "N":
			q = q.Filter(elastic.NewRangeQuery(field).Gte(parts[0]))
			q = q.Filter(elastic.NewRangeQuery(field).Lte(parts[1]))
		case "S":
			q = q.Filter(elastic.NewRangeQuery(field).Gte(parts[0]))
			q = q.Filter(elastic.NewRangeQuery(field).Lte(parts[1]))
		default:
			q = q.Filter(elastic.NewRangeQuery(field).Gte(parts[0]))
			q = q.Filter(elastic.NewRangeQuery(field).Lte(parts[1]))
		}
	}
}

func (h *SearchHandler) parseMustMatch(ctx *gin.Context, q *elastic.BoolQuery, match map[string]string) {
	for field, value := range match {
		q = q.Must(elastic.NewMatchQuery(field, value))
	}
}

func (h *SearchHandler) parseMustShould(ctx *gin.Context, q *elastic.BoolQuery, should []CCReqMustWithoutShould) {
	if len(should) > 0 {
		qs := elastic.NewBoolQuery()
		for _, s := range should {
			qt := elastic.NewBoolQuery()
			// must should need
			h.parseMustNeed(ctx, qt, s.Need)
			// must should range
			h.parseMustRange(ctx, qt, s.Range)
			// must should range
			h.parseMustMatch(ctx, qt, s.Match)

			qs = qs.Should(qt)
		}
		q = q.Must(qs)
	}
}

func (h *SearchHandler) parseMustNotNeed(ctx *gin.Context, q *elastic.BoolQuery, need map[string]string) {
	for field, value := range need {
		q = q.MustNot(elastic.NewTermsQuery(field, common.ParseStringToInterface(value)...))
	}
}

func (h *SearchHandler) parseMustNotRange(ctx *gin.Context, q *elastic.BoolQuery, rg map[string]string) {
	for field, value := range rg {
		parts := strings.Split(value, "|")
		if len(parts) != 3 {
			h.OutJson(ctx, -1, "invalid range: "+value, "")
		}
		switch parts[2] {
		case "T":
			stime := ""
			etime := ""
			if parts[0] != "" {
				tt, err := strconv.ParseInt(parts[0], 10, 64)
				if err != nil {
					h.OutJson(ctx, -1, "invalid stime "+parts[0], "")
				}
				stime = time.Unix(tt, 0).Format("2006-01-02 15:04:05")
			}
			if parts[1] != "" {
				tt, err := strconv.ParseInt(parts[1], 10, 64)
				if err != nil {
					h.OutJson(ctx, -1, "invalid etime "+parts[1], "")
				}
				etime = time.Unix(tt, 0).Format("2006-01-02 15:04:05")
			}
			qt := elastic.NewBoolQuery()
			qt = qt.Filter(elastic.NewRangeQuery(field).Format("yyyy-MM-dd HH:mm:ss").Gte(stime))
			qt = qt.Filter(elastic.NewRangeQuery(field).Format("yyyy-MM-dd HH:mm:ss").Lte(etime))
			q = q.MustNot(qt)
		case "N":
			qt := elastic.NewBoolQuery()
			qt = qt.Filter(elastic.NewRangeQuery(field).Gte(parts[0]))
			qt = qt.Filter(elastic.NewRangeQuery(field).Lte(parts[1]))
			q = q.MustNot(qt)
		case "S":
			qt := elastic.NewBoolQuery()
			qt = qt.Filter(elastic.NewRangeQuery(field).Gte(parts[0]))
			qt = qt.Filter(elastic.NewRangeQuery(field).Lte(parts[1]))
			q = q.MustNot(qt)
		default:
			qt := elastic.NewBoolQuery()
			qt = qt.Filter(elastic.NewRangeQuery(field).Gte(parts[0]))
			qt = qt.Filter(elastic.NewRangeQuery(field).Lte(parts[1]))
			q = q.MustNot(qt)
		}
	}
}

func (h *SearchHandler) parseMustNotMatch(ctx *gin.Context, q *elastic.BoolQuery, match map[string]string) {
	for field, value := range match {
		q = q.MustNot(elastic.NewMatchQuery(field, value))
	}
}

func (h *SearchHandler) parseMustNotShould(ctx *gin.Context, q *elastic.BoolQuery, should []CCReqMustWithoutShould) {
	if len(should) > 0 {
		qs := elastic.NewBoolQuery()
		for _, s := range should {
			qt := elastic.NewBoolQuery()
			// must should need
			h.parseMustNeed(ctx, qt, s.Need)
			// must should range
			h.parseMustRange(ctx, qt, s.Range)
			// must should range
			h.parseMustMatch(ctx, qt, s.Match)

			qs = qs.Should(qt)
		}
		q = q.MustNot(qs)
	}
}

func (h *SearchHandler) getTopItems(ctx *gin.Context, nc string) ([]map[string]interface{}, error) {
	items := make([]map[string]interface{}, 0)
	bdata, err := json.Marshal(h.Param.Req.Top)
	if err != nil {
		return items, err
	}
	key := fmt.Sprintf("top_%s", string(bdata))
	if nc == "yes" {
		value, err := global.G_cache["content_info"].Get(key)
		if err == nil {
			err = json.Unmarshal([]byte(value), &items)
			if err == nil {
				return items, nil
			}
		}
	}

	q := elastic.NewBoolQuery()
	h.parseMustNeed(ctx, q, h.Param.Req.Top.Need)
	query := global.G_es["yxs"].Client.Search().Index(h.esIndex).Type(h.esType).Preference("_primary_first").Timeout("1s").Query(q)
	if h.Param.Req.Top.Basic.Size != "" {
		size, _ := strconv.Atoi(h.Param.Req.Top.Basic.Size)
		if size == 0 {
			size = 10
		}
		query = query.Size(size)
	}

	if h.Param.Req.Top.Basic.Sort != "" && h.Param.Req.Top.Basic.Desc != "" {
		sorts := strings.Split(h.Param.Req.Top.Basic.Sort, ",")
		descs := strings.Split(h.Param.Req.Top.Basic.Desc, ",")
		if len(sorts) != len(descs) {
			return items, fmt.Errorf("invalid top sort and desc")
		}
		for idx, d := range descs {
			desc := true
			if d == "yes" {
				desc = false
			}
			sort := sorts[idx]
			query = query.Sort(sort, desc)
		}
	}

	resEs, errEs := query.Do(context.TODO())
	if errEs != nil {
		searchlog := ""
		src, err := q.Source()
		if err == nil {
			bdata, err := json.Marshal(src)
			if err == nil {
				searchlog = string(bdata)
			}
		}
		if global.G_debug {
			fmt.Println(searchlog)
		}
		return items, fmt.Errorf("search es for top failed. searchlog:%s", searchlog)
	}

	if global.G_debug {
		src, _ := q.Source()
		bdata, _ := json.Marshal(src)
		fmt.Println(string(bdata))
	}

	// parseDoc
	for _, hit := range resEs.Hits.Hits {
		item, err := global.G_es["yxs"].ParseDoc(hit.Source, h.Param.Res)
		if err != nil {
			return items, err
		}
		item["top"] = 1
		if h.Param.Req.Top.Basic.TimeInfo != "" {
			checkTime := common.CheckValidTime(h.Param.Req.Top.Basic.TopID,
				common.InterfaceToString(item[h.Param.Req.Top.Basic.TimeInfo]))
			if checkTime == false {
				continue
			}
		}
		if h.Param.Req.Top.Basic.WeightInfo != "" {
			item["weight"] = common.GetWeightInfo(h.Param.Req.Top.Basic.TopID,
				common.InterfaceToString(item[h.Param.Req.Top.Basic.WeightInfo]))
		}
		items = append(items, item)
	}

	if h.Param.Req.Top.Basic.WeightInfo != "" {
		common.SortItems(items, "weight")
	}

	if nc == "yes" {
		bdata, err := json.Marshal(items)
		if err == nil {
			global.G_cache["content_info"].Set(key, string(bdata))
		}
	}

	return items, nil
}

func Search(ctx *gin.Context) {
	search := &SearchHandler{}
	search.Process(ctx)
}
