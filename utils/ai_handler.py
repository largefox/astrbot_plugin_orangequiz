import json
import re

from astrbot.api import logger

QUIZ_GEN_PROMPT = """你是一个专业的心理学测试或娱乐测试出题人。你需要根据给定的标题和内容方向，生成一份问卷。
返回的格式必须是纯 JSON，不需要使用 Markdown 代码块包裹，也不要有任何其他分析和解释。

要求：
1. 请根据用户的描述判断该问卷属于【计分类(如智商测试、性格得分)】还是【完全随机分配类(如今日运势、测测你是哪种动物纯娱乐)】。如果未明确，默认使用计分类。
2. 尽量保证题目数量不要超过 6 道题。
3. 问卷标题：{title}
4. 内容设定或草稿：{content}
5. 当前人格设定：{persona_prompt}
请务必结合你当前的人格设定特征，将其无缝融入到题干描述和选项的语气中。如果没有特别的人格设定，请以专业且有趣的常规身份出题。

JSON 格式要求如下（请严格遵守二选一）：

如果是【计分类 (scoring)】：
{{
  "test_id": "", "title": "{title}", "desc": "一两句话的问卷趣味简介", "type": "scoring", "ai_tone": "{tone}",
  "questions": [
    {{
      "q_id": 1, "text": "题目内容",
      "options": [
        {{ "label": "A", "text": "选项A", "weights": {{ "KEY": 3 }} }}
      ]
    }}
  ],
  "results_logic": {{ 
    "KEY": {{ "ranges": [ {{ "min": 0, "max": 10, "name": "结果A", "desc": "详情" }} ] }}
  }}
}}

如果是【完全随机类 (random)】，不依赖选项计分，结果纯盲盒：
{{
  "test_id": "", "title": "{title}", "desc": "一两句话的问卷趣味简介", "type": "random", "ai_tone": "{tone}",
  "questions": [
    {{
      "q_id": 1, "text": "题目内容但无需 weights",
      "options": [ {{ "label": "A", "text": "选项A" }} ]
    }}
  ],
  "results_logic": {{ 
    "outcomes": [ 
      {{ "name": "结果A", "desc": "详情" }},
      {{ "name": "结果B", "desc": "详情" }}
    ] 
  }}
}}
"""


SNARKY_RESULT_PROMPT = """你是一个吐槽大师，此时正在扮演拥有【{tone}】语气的点评家。
你现在面对的是一个刚刚完成名为“{title}”测试的用户。

用户最终被系统死板地评定为：【{cat_name}】
系统的原版解析为：{base_desc}

这是用户做题时选择的真实答题轨迹：
{trajectory}

请根据得到的结果分类，给出一段结合了你【{tone}】设定以及深刻针对性的解读。
要求：
- 基于用户的答题记录进行解读，但不一定要提及具体选项。
- 长度绝对不要超过 100 字。
- 请直接输出解读正文，绝对不要带有“这是你的解读”或者问候语之类的废话结构。
"""


def extract_json(text: str) -> dict:
    """Extract the first valid JSON object from a text string."""
    text = text.strip()
    # Remove markdown code block fences if present
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text.strip())
    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Find the first '{' and scan for matching '}' to extract a balanced JSON block
    start = text.find("{")
    if start == -1:
        logger.warning("OrangeQuiz: extract_json found no '{' in LLM response.")
        return None
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                candidate = text[start : i + 1]
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError as e:
                    logger.warning(
                        f"OrangeQuiz: JSON parse failed after extraction: {e}"
                    )
                    return None
    logger.warning(
        "OrangeQuiz: extract_json could not find balanced braces in LLM response."
    )
    return None


from astrbot.api.all import Context

async def generate_quiz(
    context: Context,
    provider_id: str,
    title: str,
    content: str,
    tone: str,
    persona_prompt: str = "",
):
    persona_text = persona_prompt if persona_prompt else "无特殊设定，自由发挥。"
    prompt = QUIZ_GEN_PROMPT.format(
        title=title, content=content, tone=tone, persona_prompt=persona_text
    )
    resp = await context.llm_generate(chat_provider_id=provider_id, prompt=prompt)
    if not resp or not resp.completion_text:
        return None
    return extract_json(resp.completion_text)


async def generate_snarky_eval(
    context: Context,
    provider_id: str,
    quiz_title: str,
    cat_name: str,
    base_desc: str,
    trajectory: str,
    tone: str = "毒舌犀利",
    persona_prompt: str = "",
):
    sys_instruction = ""
    if persona_prompt:
        sys_instruction = f"当前，你正扮演以下人格设定，请务必保持此人设结合【{tone}】的语气来进行点评：\n{persona_prompt}\n\n"

    prompt = sys_instruction + SNARKY_RESULT_PROMPT.format(
        title=quiz_title,
        cat_name=cat_name,
        base_desc=base_desc,
        trajectory=trajectory,
        tone=tone,
    )
    resp = await context.llm_generate(chat_provider_id=provider_id, prompt=prompt)
    if not resp or not resp.completion_text:
        return "（由于神秘力量干扰，本大师现在不想吐槽你，算你好运。）"
    return resp.completion_text.strip()
