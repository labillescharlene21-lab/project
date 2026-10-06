(function () {
  "use strict";
  var D = window.SITE_DATA;
  var NS = "http://www.w3.org/2000/svg";
  var notice = document.getElementById("notice");
  if (!D) {
    notice.hidden = false;
    notice.textContent = "Dashboard data not found. Run: python -m src.analysis.export_site";
    return;
  }

  // Brand colours. Severity scale: Low = Tea Green, Moderate = Apricot Peach, High = Indian Red, Critical = Sunset Orange.
  var C = { slate: "#193C40", cyan: "#3A8C95", smoke: "#F3F3F3" };
  var SEV = { low: "#E1FCC4", moderate: "#FCC4C5", high: "#C1686A", critical: "#F95454", none: "#F3F3F3" };
  var SEV_ORDER = { none: 0, low: 1, moderate: 2, high: 3, critical: 4 };   // draw order: most severe on top
  var AREA = { us: "United States", europe: "Europe", gemstat_latin_america: "Latin America" };
  var REALM = { freshwater: "Fresh water", marine: "Marine" };
  var fmt = new Intl.NumberFormat("en-US");

  function el(tag, props, kids) {
    var e = document.createElement(tag);
    Object.keys(props || {}).forEach(function (k) {
      if (k === "text") e.textContent = props[k]; else e.setAttribute(k, props[k]);
    });
    (kids || []).forEach(function (c) { e.appendChild(typeof c === "string" ? document.createTextNode(c) : c); });
    return e;
  }
  function icon(name) {
    var s = document.createElementNS(NS, "svg");
    s.setAttribute("class", "icon"); s.setAttribute("aria-hidden", "true"); s.setAttribute("focusable", "false");
    var u = document.createElementNS(NS, "use");
    u.setAttribute("href", "#i-" + name);
    s.appendChild(u);
    return s;
  }
  function pct(x, d) { return x == null ? "–" : (x * 100).toFixed(d || 0) + "%"; }
  function num(x, d) { return x == null ? "–" : Number(x).toFixed(d == null ? 2 : d); }
  function area(s) { return AREA[s] || s || "–"; }
  function water(r) { return el("span", { "class": "chip " + (r === "marine" ? "marine" : "fresh"), text: REALM[r] || r || "–" }); }

  var m = D.meta;
  if (m.weather_coverage != null && m.weather_coverage < 0.9) {
    notice.hidden = false;
    notice.textContent = "Rainfall data covers " + pct(m.weather_coverage) + " of the measurements so far, so rain-related scores are provisional.";
  }

  // At a glance
  [["flask", m.observations == null ? "–" : fmt.format(m.observations), "measurements analysed, " + m.period],
   ["pin", fmt.format(m.sites), "sites on the map"],
   ["drop", fmt.format(m.hotspots), "persistent hotspots (" + pct(m.hotspot_share) + " of scored sites)"],
   ["layers", fmt.format(m.regions_ranked), "regions ranked"],
   ["rain", pct(m.weather_coverage), "of measurements have rainfall data"]
  ].forEach(function (k) {
    document.getElementById("kpis").appendChild(el("div", { "class": "kpi" }, [icon(k[0]), el("b", { text: k[1] }), el("span", { text: k[2] })]));
  });

  // Top 10 regions
  var regions = document.getElementById("regions");
  if (!D.top_regions.length) {
    regions.parentNode.insertBefore(el("p", { "class": "empty", text: "No region has enough monitored sites to be ranked yet." }), regions);
  }
  var maxScore = Math.max.apply(null, D.top_regions.map(function (r) { return r.score; }).concat([0.0001]));
  D.top_regions.forEach(function (r) {
    regions.appendChild(el("li", { "class": "rank-card" }, [
      el("span", { "class": "rank-num", text: r.rank }),
      el("div", {}, [
        el("div", { "class": "rank-head" }, [el("strong", { text: r.region }), el("span", { text: r.country })]),
        el("div", { "class": "track" }, [el("div", { "class": "fill", style: "width:" + (100 * r.score / maxScore).toFixed(1) + "%" })]),
        el("p", { "class": "caption", text: r.hotspots + " of " + r.sites + " monitored sites are persistent hotspots · score " + r.score.toFixed(2) })
      ])
    ]));
  });

  // Top sites
  var tb = document.querySelector("#sites-table tbody");
  D.top_sites.forEach(function (s, i) {
    tb.appendChild(el("tr", {}, [
      el("td", { text: i + 1 }), el("td", { text: s.site }),
      el("td", { text: (s.region || "–") + (s.country ? ", " + s.country : "") }), el("td", {}, [water(s.realm)]),
      el("td", { "class": "num", text: pct(s.exceedance_rate) }), el("td", { "class": "num", text: num(s.ratio, 1) + "×" }),
      el("td", { "class": "num", text: s.poor_years == null ? "–" : s.poor_years })]));
  });

  // Areas
  var sb = document.querySelector("#strata-table tbody");
  D.strata.forEach(function (s) {
    sb.appendChild(el("tr", {}, [
      el("td", { text: area(s.stratum) }), el("td", {}, [water(s.realm)]),
      el("td", { "class": "num", text: s.sites }), el("td", { "class": "num", text: s.hotspots }),
      el("td", { "class": "num", text: pct(s.share) }), el("td", { "class": "num", text: num(s.median_ratio, 2) + "×" })]));
  });

  // Map: dark slate base, Dark Cyan waterways, severity-coloured circles (bigger = more days above the limit)
  var map = L.map("map", { minZoom: 2, maxZoom: 10, zoomSnap: 0.5, worldCopyJump: false,
                           maxBounds: [[-62, -185], [85, 185]], maxBoundsViscosity: 1, attributionControl: false });
  L.control.attribution({ prefix: '<a href="https://leafletjs.com">Leaflet</a>' }).addAttribution("Base map: Natural Earth").addTo(map);
  if (window.BASEMAP) {
    L.geoJSON(window.BASEMAP.countries, { interactive: false,
      style: { color: C.cyan, weight: 0.7, opacity: 0.5, fillColor: C.slate, fillOpacity: 1 } }).addTo(map);
    L.geoJSON(window.BASEMAP.rivers, { interactive: false,
      style: { color: C.cyan, weight: 1.3, opacity: 0.95, fill: false } }).addTo(map);
  }
  var layer = L.layerGroup().addTo(map);
  var sites = D.sites.slice().sort(function (a, b) { return SEV_ORDER[a.severity] - SEV_ORDER[b.severity]; });

  var sel = document.getElementById("f-stratum");
  Object.keys(D.sites.reduce(function (a, s) { a[s.stratum] = 1; return a; }, {})).sort().forEach(function (k) {
    sel.appendChild(el("option", { value: k, text: area(k) }));
  });

  var legend = document.getElementById("legend");
  D.legend.forEach(function (it) {
    var sw = el("span", { "class": "sw" + (it.key === "none" ? " none" : ""), style: "background:" + (it.key === "none" ? "transparent" : SEV[it.key]) });
    var cb = el("input", { type: "checkbox", id: "f-" + it.key, checked: "checked" });
    cb.addEventListener("change", function () { draw(false); });
    legend.appendChild(el("label", { "for": "f-" + it.key }, [cb, el("span", {}, [sw, el("b", { text: it.label })]), el("small", { text: it.text })]));
  });

  function popup(s) {
    var box = el("div", {});
    box.appendChild(el("strong", { text: s.site_key }));
    [["Area", area(s.stratum) + " · " + (REALM[s.realm] || s.realm)],
     ["Region", (s.admin1_name || "–") + (s.country_name ? ", " + s.country_name : "")],
     ["Severity", (D.legend.filter(function (l) { return l.key === s.severity; })[0] || {}).label || "–"],
     ["Days above limit", pct(s.exceedance_rate)],
     ["Typical ÷ limit", s.median_ratio_to_threshold == null ? "–" : num(s.median_ratio_to_threshold, 2) + "×"],
     ["Poor years", s.poor_years == null ? "–" : s.poor_years + " of last " + (s.years_monitored == null ? "?" : Math.min(5, s.years_monitored))]
    ].forEach(function (p) { box.appendChild(el("div", { text: p[0] + ": " + p[1] })); });
    return box;
  }

  function draw(fit) {
    layer.clearLayers();
    var st = sel.value, on = {}, pts = [];
    D.legend.forEach(function (l) { on[l.key] = document.getElementById("f-" + l.key).checked; });
    sites.forEach(function (s) {
      if ((st && s.stratum !== st) || !on[s.severity]) return;
      var none = s.severity === "none";
      var rate = s.exceedance_rate == null ? 0 : Math.min(1, s.exceedance_rate);
      layer.addLayer(L.circleMarker([s.latitude, s.longitude], {
        radius: none ? 4 : 4 + 8 * rate, color: none ? C.smoke : C.slate, weight: 1, opacity: none ? 0.7 : 1,
        fillColor: SEV[s.severity], fillOpacity: none ? 0 : 0.92
      }).bindPopup(popup(s)));
      pts.push([s.latitude, s.longitude]);
    });
    document.getElementById("map-count").textContent = fmt.format(pts.length) + " sites shown";
    if (fit && pts.length) map.fitBounds(pts, { padding: [24, 24], maxZoom: 7 });
  }
  sel.addEventListener("change", function () { draw(true); });
  map.setView([20, 0], 2);
  draw(true);

  document.getElementById("stamp").textContent = "Data generated " + m.generated_at + " · curated batch " + m.curated_batch + ".";
})();
