.PHONY: update-css
update-css:
	npm i -D daisyui@latest "@tailwindcss/typography@latest" tailwindcss-bg-patterns@latest
	
.PHONY: mini
mini: 
	npx tailwindcss -i ./input.css -o public/tailwind.css --minify
