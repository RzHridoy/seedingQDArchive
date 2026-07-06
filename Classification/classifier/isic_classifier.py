import sys, os, re, json, time, requests
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.database import (get_all_projects, get_project_keywords,
                          update_project_classification)
from classifier.isic_taxonomy import KEYWORD_MAP, DIVISION_BY_CODE, label

API_URL = "https://api.anthropic.com/v1/messages"
MODEL   = "model"
API_DELAY      = 0.3
TARGET_TYPES   = {"QDA_PROJECT", "QD_PROJECT"}

_SYSTEM = (
    "You are an ISIC Rev.5 expert. Given a research project title, description, "
    "and keywords, return ONLY a JSON object — no markdown, no explanation:\n"
    '{"primary":"72","secondary":"86","tags":["tag1","tag2"]}\n'
    "Use two-digit ISIC division codes. Set secondary to null if unsure."
)


def _strip_html(text):
    return re.sub(r"<[^>]+>", " ", text or "").strip()


def _build_text(project, keywords):
    title = project.get("title") or ""
    desc  = _strip_html(project.get("description") or "")[:1500]
    return f"{title} {' '.join(keywords)} {desc}".lower()


def _extract_tags(project, keywords):
    tags = set()
    stop = {"the","a","an","of","in","and","or","for","to","with","on","at",
            "by","from","as","is","are","was","were","be","been","data","this"}
    for kw in keywords:
        if 3 < len(kw.strip()) < 60:
            tags.add(kw.strip().lower())
    for word in re.sub(r"[^a-z0-9 ]", " ",
                       (project.get("title") or "").lower()).split():
        if word not in stop and len(word) > 3:
            tags.add(word)
    return sorted(tags)[:25]


def _heuristic(text):
    hits = Counter()
    for kw, code in KEYWORD_MAP.items():
        if kw in text:
            hits[code] += 1
    if not hits:
        return None, None, 0
    top = hits.most_common(3)
    primary   = top[0][0]
    secondary = top[1][0] if len(top) >= 2 and top[1][1] >= 2 else None
    return primary, secondary, top[0][1]


def _api_classify(project, keywords):
    title = project.get("title") or "Untitled"
    desc  = _strip_html(project.get("description") or "")[:800]
    kws   = ", ".join(keywords[:15]) or "none"
    msg   = f"Title: {title}\nDescription: {desc}\nKeywords: {kws}"
    try:
        r = requests.post(
            API_URL,
            headers={"Content-Type": "application/json"},
            json={"model": MODEL, "max_tokens": 200,
                  "system": _SYSTEM,
                  "messages": [{"role": "user", "content": msg}]},
            timeout=30
        )
        if r.status_code != 200:
            return None, None, []
        text = r.json()["content"][0]["text"].strip()
        text = re.sub(r"^```[a-z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
        d    = json.loads(text)
        pri  = str(d.get("primary") or "").strip() or None
        sec  = str(d.get("secondary") or "").strip() or None
        if sec in ("null", "None", ""):
            sec = None
        tags = [str(t).strip().lower() for t in (d.get("tags") or []) if t]
        if pri and pri not in DIVISION_BY_CODE: pri = None
        if sec and sec not in DIVISION_BY_CODE: sec = None
        return pri, sec, tags
    except Exception:
        return None, None, []


def run(use_api=True, api_threshold=2):
    print("\n" + "=" * 60)
    print("Step 2 & 3 — ISIC Classification")
    print(f"  API: {'enabled' if use_api else 'disabled (heuristic only)'}")
    print("=" * 60)

    projects = get_all_projects()
    target   = [p for p in projects if p.get("project_type") in TARGET_TYPES]
    print(f"  Projects to classify: {len(target)}")

    api_n = heur_n = fail_n = 0

    for i, p in enumerate(target, 1):
        pid      = p["id"]
        keywords = get_project_keywords(pid)
        text     = _build_text(p, keywords)
        tags     = _extract_tags(p, keywords)

        h_pri, h_sec, hits = _heuristic(text)

        if h_pri and hits >= api_threshold:
            primary, secondary = h_pri, h_sec
            heur_n += 1
        elif use_api:
            a_pri, a_sec, a_tags = _api_classify(p, keywords)
            time.sleep(API_DELAY)
            api_n += 1
            if a_pri:
                primary, secondary = a_pri, a_sec
                if a_tags:
                    tags = a_tags
            elif h_pri:
                primary, secondary = h_pri, h_sec
            else:
                primary, secondary = "72", None
                fail_n += 1
        else:
            primary   = h_pri or "72"
            secondary = h_sec

        update_project_classification(
            pid,
            label(primary)   if primary   else None,
            label(secondary) if secondary else None,
            json.dumps(tags)
        )

        if i % 50 == 0 or i == len(target):
            print(f"  [{i:>4}/{len(target)}] heuristic={heur_n}  api={api_n}  default={fail_n}")

    print(f"\n  Done. Heuristic={heur_n} | API={api_n} | Defaulted={fail_n}")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--no-api", action="store_true")
    args = p.parse_args()
    run(use_api=not args.no_api)
