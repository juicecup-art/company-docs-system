# 公司文档管理系统交接文档

## 系统概述

本系统是一个基于FastAPI + SQLAlchemy Core的公司文档管理系统，主要功能包括公司管理、文档管理、平台管理等模块。

## 核心架构

### 1. 数据库连接和查询

**数据库连接文件**：[db.py](app/db.py)
- 使用SQLAlchemy Core进行数据库操作
- 连接池管理和事务处理
- 支持MySQL风格SQL（使用NOW()和LAST_INSERT_ID()）

**查询模式**：
```python
with engine.begin() as conn:  # 事务处理
    result = conn.execute(
        text("SELECT * FROM table WHERE id=:id"),
        {"id": record_id}
    ).mappings().all()  # 返回字典列表
```

### 2. 路由系统和注册

**主路由文件**：[main.py](app/main.py)
- 包含基础CRUD API接口
- 路由注册顺序很重要：UI路由优先注册避免冲突

**UI路由文件**：
- [ui.py](app/routers/ui.py) - 主要UI页面路由
- [ui_pack.py](app/routers/ui_pack.py) - 包装法相关功能
- [ui_tickets.py](app/routers/ui_tickets.py) - 工单系统

**路由注册顺序**（重要！）：
```python
# UI路由优先注册，避免与API路由冲突
app.include_router(ui_router)
app.include_router(ui_pack_router)
app.include_router(ui_tickets_router)
# 然后是API路由
app.include_router(admin_router)
app.include_router(auth_router)
# ...其他路由
```

### 3. 数据传递流程

**公司详情页数据传递**（以platforms为例）：

1. **数据库查询**：[ui.py#L1815-L1835](app/routers/ui.py#L1815-L1835)
```python
platforms = conn.execute(
    text("""
        SELECT id AS platform_id, company_id, platform_name, 
               packing_name, payment_name, platform_email, 
               progress, created_at
        FROM company_platforms
        WHERE company_id=:cid
        ORDER BY created_at DESC, platform_id DESC
        LIMIT 50
    """),
    {"cid": company_id},
).mappings().all()
```

2. **数据传递给模板**：[ui.py#L1870](app/routers/ui.py#L1870)
```python
return templates.TemplateResponse(
    "company_detail.html",
    {
        # ...其他数据
        "platforms": platforms,  # 关键：传递platforms数据
        # ...其他数据
    },
)
```

3. **模板接收和处理**：[company_detail.html#L470](app/templates/company_detail.html#L470)
```jinja2
{% set ns = namespace(ecom=[], pay=[]) %}
{% for p in platforms %}
    {% set pn = (p.payment_name or '')|lower %}
    {% set n = (p.platform_name or '')|lower %}
    {% if pn %}
        {% set _ = ns.pay.append(p) %}
    {% elif n in pay_names %}
        {% set _ = ns.pay.append(p) %}
    {% else %}
        {% set _ = ns.ecom.append(p) %}
    {% endif %}
{% endfor %}
```

4. **模板显示逻辑**：
```jinja2
{{ p.packing_name or p.platform_name or 'Unknown Platform' }}
```

## 包装法数据问题分析

### 问题现象
包装法数据在数据库中存在，但在页面上显示为None或空白。

### 根本原因
1. **数据分类逻辑**：包装法记录的`platform_name=None`，`packing_name='瑞士包装法'`
2. **显示逻辑**：`{{ p.platform_name }}`会显示None，应该使用`{{ p.packing_name or p.platform_name }}`

### 解决方案
修复模板显示逻辑：
```jinja2
{# 修复前 #}
{{ p.platform_name }}

{# 修复后 #}
{{ p.packing_name or p.platform_name or 'Unknown Platform' }}
```

## 关键文件位置

### 核心文件
- **主应用**：[main.py](app/main.py)
- **数据库连接**：[db.py](app/db.py)
- **UI路由**：[routers/ui.py](app/routers/ui.py)
- **包装法功能**：[routers/ui_pack.py](app/routers/ui_pack.py)

### 模板文件
- **公司详情页**：[templates/company_detail.html](app/templates/company_detail.html)
- **基础模板**：[templates/base.html](app/templates/base.html)

### 数据模型
- **公司表操作**：查看main.py中的Company相关API
- **平台表操作**：company_platforms表相关查询

## 开发注意事项

### 1. 路由冲突避免
- UI路由（/ui/*）优先注册
- API路由和UI路由路径要区分清楚
- 使用`response_class=HTMLResponse`标记UI路由

### 2. 数据查询最佳实践
- 使用`.mappings().all()`获取字典列表
- 使用参数化查询防止SQL注入
- 字段别名要清晰（如`id AS platform_id`）

### 3. 模板数据处理
- 处理None值：`{{ value or '默认值' }}`
- 分类逻辑要明确（电商平台 vs 收款平台）
- 调试信息用完后及时清理

### 4. 包装法特殊处理
- 包装法记录：`platform_name=None`, `packing_name='具体包装法名称'`
- 普通平台：`platform_name='平台名称'`, `packing_name=None`
- 显示时统一使用：`{{ packing_name or platform_name }}`

### 5. 调试技巧
- 在路由函数中添加`print()`输出调试信息
- 在模板中添加调试div，用不同背景色区分
- 检查数据库原始数据是否正确
- 验证数据传递链路的每个环节

## 系统特点

1. **前后端分离**：FastAPI提供API，Jinja2模板渲染HTML
2. **SQLAlchemy Core**：轻量级ORM，直接SQL控制
3. **模块化设计**：路由、模型、服务分离
4. **权限控制**：基于用户角色的权限验证
5. **软删除机制**：逻辑删除而非物理删除

## 常见问题排查

### 数据不显示
1. 检查数据库查询是否返回数据
2. 验证数据是否正确传递给模板
3. 检查模板显示逻辑是否正确
4. 确认路由没有被覆盖或冲突

### 页面显示异常
1. 检查模板语法是否正确
2. 验证变量名是否匹配
3. 检查CSS样式是否影响显示
4. 查看浏览器控制台错误信息

### 路由问题
1. 检查路由注册顺序
2. 验证路径参数是否正确
3. 检查响应类型设置（HTML vs JSON）
4. 确认权限验证是否通过

---

**最后更新**：2026年3月6日  
**文档状态**：完整可用  
**适用版本**：当前系统版本