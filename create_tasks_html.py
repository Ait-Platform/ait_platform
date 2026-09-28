import os

filepath = 'templates/program_uip/subcomm_tools/tasks.html'
html = '''{% extends "program_uip/base.html" %}
{% block title %}{{ subcommittee.name }} - Tasks & Actions{% endblock %}

{% block content %}
<div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
    <div class="mb-8">
        <a href="{{ url_for('uip_bp.sub_comm_board', org_slug=org.slug, sub_id=subcommittee.id) }}" class="text-sm font-medium text-slate-500 hover:text-slate-700 mb-4 inline-block">&larr; Back to Command Centre</a>
        <h1 class="text-3xl font-black text-slate-800 tracking-tight">{{ subcommittee.name }} Tasks & Routed Queries</h1>
        <p class="text-lg text-slate-500 mt-2">Manage infrastructure faults, queries, and tasks assigned to your subcommittee.</p>
    </div>

    <div class="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
        <table class="min-w-full divide-y divide-slate-200">
            <thead class="bg-slate-50">
                <tr>
                    <th scope="col" class="px-6 py-3 text-left text-xs font-bold text-slate-500 uppercase tracking-wider">Task</th>
                    <th scope="col" class="px-6 py-3 text-left text-xs font-bold text-slate-500 uppercase tracking-wider">Related Query</th>
                    <th scope="col" class="px-6 py-3 text-left text-xs font-bold text-slate-500 uppercase tracking-wider">Due</th>
                    <th scope="col" class="px-6 py-3 text-left text-xs font-bold text-slate-500 uppercase tracking-wider">Status</th>
                    <th scope="col" class="px-6 py-3 text-right text-xs font-bold text-slate-500 uppercase tracking-wider">Action</th>
                </tr>
            </thead>
            <tbody class="bg-white divide-y divide-slate-200">
                {% for task in tasks %}
                <tr class="hover:bg-slate-50 transition-colors">
                    <td class="px-6 py-4">
                        <div class="text-sm font-bold text-slate-900">{{ task.title }}</div>
                        <div class="text-sm text-slate-500">{{ task.description or '' }}</div>
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap">
                        <a href="{{ url_for('uip_bp.reception_issue', org_slug=org.slug, issue_id=task.interaction_id) }}" class="text-sm font-medium text-blue-600 hover:text-blue-800">
                            {{ task.interaction.reference or ('Query #' ~ task.interaction_id) }}
                        </a>
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-500">
                        {{ task.due_date.strftime('%Y-%m-%d') if task.due_date else 'No Date' }}
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap">
                        <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-amber-100 text-amber-800">
                            {{ task.status }}
                        </span>
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                        <a href="{{ url_for('uip_bp.reception_issue', org_slug=org.slug, issue_id=task.interaction_id) }}" class="text-indigo-600 hover:text-indigo-900">Manage</a>
                    </td>
                </tr>
                {% else %}
                <tr>
                    <td colspan="5" class="px-6 py-12 text-center text-slate-500 text-sm">
                        No active tasks or routed queries for this subcommittee.
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
</div>
{% endblock %}'''

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(html)

print("Created tasks.html")
