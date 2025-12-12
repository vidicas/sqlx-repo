.PHONY: update-css
update-css:
	npm i -D daisyui@latest "@tailwindcss/typography@latest" tailwindcss-bg-patterns@latest
	
.PHONY: mini-css
mini-css: 
	npx @tailwindcss/cli -i ./input.css -o public/tailwind.css --minify

.PHONY: watch-css
watch-css:
	npx @tailwindcss/cli -i ./input.css -o ./public/tailwind.css --watch
