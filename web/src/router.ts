/**
 * Five pages (01 §5.3.5). Hash history keeps deep links working behind any static file
 * server (the api-gateway need not implement an SPA fallback).
 */
import { createRouter, createWebHashHistory, type RouteRecordRaw } from 'vue-router'

const routes: RouteRecordRaw[] = [
  { path: '/', name: 'overview', component: () => import('@/views/OverviewPage.vue'), meta: { title: '总览' } },
  { path: '/accounts/:id?', name: 'account', component: () => import('@/views/AccountPage.vue'), meta: { title: '账户' } },
  { path: '/compare', name: 'compare', component: () => import('@/views/ComparePage.vue'), meta: { title: '多账户对比' } },
  { path: '/market', name: 'market', component: () => import('@/views/MarketSystemPage.vue'), meta: { title: '行情与系统' } },
  { path: '/reports', name: 'reports', component: () => import('@/views/ReportsPage.vue'), meta: { title: '报表' } },
  { path: '/:pathMatch(.*)*', redirect: '/' },
]

export const router = createRouter({
  history: createWebHashHistory(),
  routes,
  scrollBehavior: () => ({ top: 0 }),
})

router.afterEach((to) => {
  const t = typeof to.meta.title === 'string' ? to.meta.title : ''
  document.title = t ? `${t} · futrisk 风控仪表盘` : 'futrisk 风控仪表盘'
})
