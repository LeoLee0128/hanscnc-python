import os, re, json, time, hmac, base64, hashlib
from datetime import timezone, timedelta
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from tenacity import retry, wait_exponential, stop_after_attempt

BASE_URL = "https://www.hanscnc.com/investorsreport/list.html"
SOURCE_NAME = "大族数控-定期报告"
STATE_PATH = "data/state.json"
TIMEZONE = timezone(timedelta(hours=8))
os.makedirs("data", exist_ok=True)

def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"seen_hrefs": []}

def save_state(state):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def norm_text(s):
    return re.sub(r"\s+", " ", (s or "").strip())

@retry(wait=wait_exponential(multiplier=1, min=1, max=10), stop=stop_after_attempt(3))
def fetch_html(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (monitor; +https://github.com/) requests",
        "Accept": "text/html,application/xhtml+xml"
    }
    with requests.Session() as s:
        s.headers.update(headers)
        resp = s.get(url, timeout=15)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
        return resp.text

def near_text(node):
    texts = []
    for p in [node, node.parent] + list(node.parents)[:2]:
        if not p:
            continue
        texts.append(norm_text(p.get_text(" ")))
    return " ".join(texts)

def extract_items(html, base_url, top_n=20):
    soup = BeautifulSoup(html, "lxml")
    items, seen = [], set()
    anchors = [a for a in soup.find_all("a") if "下载" in norm_text(a.get_text())]
    for a in anchors:
        href = a.get("href") or ""
        if not href:
            continue
        href = urljoin(base_url, href)
        title, date_str = None, None
        for up in [a, a.parent, a.parent.parent if a.parent else None]:
            if not up:
                continue
            cand = up.find_all(["h3","h4","a","p","span"], limit=8)
            for n in cand:
                t = norm_text(n.get_text())
                if (not title) and ("报告" in t or "季度" in t or "半年度" in t or "年度" in t):
                    title = t
                if not date_str:
                    m = re.search(r"(\d{4}-\d{2}-\d{2})", t)
                    if m:
                        date_str = m.group(1)
                if title and date_str:
                    break
            if title and date_str:
                break
        if not date_str:
            big = near_text(a)
            m = re.search(r"(\d{4}-\d{2}-\d{2})", big)
            if m:
                date_str = m.group(1)
        if not title:
            for h in soup.find_all(["h3","h4"]):
                t = norm_text(h.get_text())
                if "报告" in t:
                    title = t
                    break
        if href in seen:
            continue
        seen.add(href)
        items.append({"title": title or "未识别标题", "date": date_str or "", "href": href})
    if not items:
        for h in soup.find_all(["h3","h4","a"]):
            t = norm_text(h.get_text())
            if "报告" in t:
                a = h if h.name == "a" else h.find("a")
                href = urljoin(base_url, a.get("href")) if a and a.get("href") else base_url
                m = re.search(r"(\d{4}-\d{2}-\d{2})", t)
                items.append({"title": t, "date": m.group(1) if m else "", "href": href})
    return items[:top_n]

def feishu_sign(ts, secret):
    string_to_sign = f"{ts}\n{secret}".encode("utf-8")
    h = hmac.new(secret.encode("utf-8"), string_to_sign, digestmod=hashlib.sha256)
    return base64.b64encode(h.digest()).decode("utf-8")

def notify_feishu(items):
    webhook = os.getenv("FEISHU_WEBHOOK_URL", "").strip()
    if not webhook:
        return
    secret = os.getenv("FEISHU_SECRET", "").strip()
    lines = [f"- {it['title']}（{it.get('date','')}）\n  {it['href']}" for it in items]
    content = f"{SOURCE_NAME} 发现新报告：\n" + "\n".join(lines) + f"\n来源：{BASE_URL}"
    payload = {"msg_type": "text", "content": {"text": content}}
    ts = str(int(time.time()))
    headers = {"Content-Type": "application/json; charset=utf-8"}
    if secret:
        payload["timestamp"] = ts
        payload["sign"] = feishu_sign(ts, secret)
    resp = requests.post(webhook, headers=headers, data=json.dumps(payload), timeout=10)
    resp.raise_for_status()

def main():
    top_n = int(os.getenv("TOP_N", "20"))
    force_notify = str(os.getenv("FORCE_NOTIFY", "")).lower() in {"1","true","yes"}
    state = load_state()
    html = fetch_html(BASE_URL)
    items = extract_items(html, BASE_URL, top_n=top_n)
    seen = set(state.get("seen_hrefs", []))
    new_items = [it for it in items if it["href"] not in seen]

    if not seen and items and not force_notify:
        state["seen_hrefs"] = [it["href"] for it in items]
        save_state(state)
        print("Initialized state; no notifications sent.")
        return

    if force_notify:
        targets = items[:1] if items else []
        if targets:
            try:
                notify_feishu(targets)
            except Exception as e:
                print("Feishu notify failed:", e)
            print("Force-notify sent (state not changed).")
        else:
            print("No items available for force notify.")
        return

    if not new_items:
        print("No new items.")
        return

    try:
        notify_feishu(new_items)
    except Exception as e:
        print("Feishu notify failed:", e)

    state["seen_hrefs"] = list(seen.union([it["href"] for it in new_items]))
    save_state(state)
    print(f"Notified {len(new_items)} new items.")

if __name__ == "__main__":
    main()
