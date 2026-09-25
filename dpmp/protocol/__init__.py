# -*- coding: utf-8 -*-
"""DPMP 协议层：常量、编解码、地址工具。

本层是两端（Python / Kotlin）实现共同遵循的【唯一事实来源】。
任何字段、端口、报文类型的改动，都必须先改本层并同步到
DPMP_PROTOCOL.md 与 test_vectors.json。
"""
