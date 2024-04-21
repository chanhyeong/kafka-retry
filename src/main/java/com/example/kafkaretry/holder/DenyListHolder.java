package com.example.kafkaretry.holder;

import java.util.HashSet;
import java.util.Set;

import org.springframework.stereotype.Component;

@Component
public class DenyListHolder {

	private final Set<String> ids = new HashSet<>();

	public boolean exists(String id) {
		return ids.contains(id);
	}

	public void add(String id) {
		ids.add(id);
	}
}
